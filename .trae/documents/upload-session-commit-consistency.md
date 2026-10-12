# 上传会话唯一提交与远端消息收据（Remote Message Receipt）方案

## Context（为什么改）

分块上传在「Telegram 已发布消息」与「数据库落库」之间存在崩溃/歧义窗口：

1. **StorePart 歧义提交后误删**：[upload.go](file:///Users/ding/Documents/swe/0910111/project-04/internal/transfer/upload.go#L245-L248) 对 StorePart 的**任何**错误都无条件 `DeleteMessages`。若提交实际成功而响应丢失，会删掉已被 stored part（甚至 file_parts）引用的消息，产出损坏文件。
2. **发布成功但 StorePart 失败**：part 行无 message_id，消息无任何持久记录，客户端重试会**再次发布**（重复消息泄漏，14 天 orphan GC 才兜底）。
3. **断连/Abort/过期竞态**：最后一个分块发布后客户端取消、Abort、Complete 重试或进程重启时，远端消息与 part/file/session 状态可能只成功一半；缺少可恢复的关联记录。
4. 过期租约的迟到 uploader 当前靠 part 行 lease_token 防护（已安全），但新机制必须把同样的防护延伸到收据，且迟到者绝不能删除新 owner 的消息。

目标：为每次 claim 建立**持久化远端消息收据**，使每个会话形成唯一提交结果，重试读原结果而非重复发布；清理按「尚未发布 / 已提交 / 可安全删除」三类收敛；零字节、冲突策略、加密分块、校验失败、正常单次完成的可观察行为（HTTP 200/201/409/410/422、错误码）保持不变。

## 设计总览：upload_part_receipts 生命周期

每次 claim（一个 lease_token = 一次尝试）一行收据，四态映射用户要求的三类资源：

```
pending ──RecordPartPublication──► published ──StorePart 同事务──► adopted（已提交）
                                      │
                                      └──校验失败/租约丢失作废──► deleting（可安全删除）
```

- **尚未发布** = `pending`（message_id 为空）：租约死亡后仅删行，不调 Telegram。
- **已提交** = `adopted`（part stored 同事务翻转）：消息由 upload_parts/file_parts 引用，永不因收据删除；仅做行级 GC。
- **可安全删除** = `deleting`、或 published 但会话已 aborted/expired、或被不同 message 的 stored part 取代；删除前用双重 `NOT EXISTS` 守卫确认未被 upload_parts/file_parts 引用。

关键不变量：
- 收据所有写入由**收据自己的 lease_token** 守护，不依赖 part 行当前租约 → 迟到 uploader 的作废/记录操作命中 0 行，不可能删掉新 owner 的消息。
- StorePart 为**单事务**：part→stored 与收据→adopted 原子提交。
- 接管（adoption）= 同事务内 DELETE 旧 published 收据 + INSERT 新 lease 的 published 收据（租约转移），随后 part 行在新 token 上存储。
- 收据**不加入** orphan GC 的引用集合（ListReferencedMessageIDs 不改）；adopted 消息本就由 upload_parts/file_parts 保护。

## 改动清单

### 1. 新迁移 `db/migrations/000009_upload_part_receipts.sql`

- 新 enum `upload_part_receipt_state`（pending/published/adopted/deleting）。
- 新表 `upload_part_receipts`：
  - PK `(upload_id, part_no, lease_token)`；`upload_id` FK→upload_sessions `ON DELETE CASCADE`；**不** FK→upload_parts（清理须先删消息再删 part 行）。
  - 列：`channel_id, plain_size` NOT NULL；`message_id, stored_size, checksum, salt, block_hashes, published_at` 可空；`state, lease_expires_at, created_at, updated_at`。
  - CHECK：part_no>0、plain_size≥0、stored_size≥0；publication 约束（pending ⇔ message/stored/published_at 全空；其余三态 ⇔ 全非空）。
  - 部分唯一索引 `(channel_id, message_id) WHERE message_id IS NOT NULL`；pending 过期索引；`(upload_id, part_no, state)` 索引；deleting 扫描索引。
  - 沿用 `/* TEMPLATE: schema */` 标记，格式参照 [000002](file:///Users/ding/Documents/swe/0910111/project-04/db/migrations/000002_part_block_hashes.sql)。
- **回填**：为现有 upload_parts 每一行插一条收据——stored→adopted（带 message/校验信息/published_at=now()），其余→pending（lease_token 用 COALESCE(part.lease_token, gen_random_uuid())，lease_expires_at COALESCE(now())），保证部署瞬间在途上传不会因新 CTE 报 ErrLeaseLost。
- Down：DROP TABLE → DROP TYPE。

### 2. `db/queries/uploads.sql` 新增/修改查询

写库后运行 `just generate-db`（sqlc + patchsqlc），不得手改 `internal/db/sqlcgen`。

租约/claim：
- `InsertPartReceipt :exec`（pending 行，fresh token 必无冲突）。
- `LockUploadPart :one`（`FOR UPDATE`）。
- `FindTakeablePartReceipt :one`：published + message 非空 + 会话 open 未过期 + part 未 stored +（收据 token≠part 当前 token 或收据租约过期）+ plain_size 相等；`ORDER BY published_at DESC LIMIT 1`。
- `ClaimUploadPartOnReceipt :one`：接管路径把 part 行 channel 改为收据 channel 并挂新租约（WHERE state<>'stored' 且旧租约死亡）。
- `InsertTakeoverReceipt :exec` / `DeletePartReceiptByLease :exec`（租约转移，同事务）。
- 改 `RenewUploadPartLease :execrows`：CTE 内同时 UPDATE 收据与 part 的 lease_expires_at，主语句仍是 part UPDATE（rows-affected 语义不变）。
- 改 `MarkUploadPartFailed :one`：前导 CTE 仅删除该 token 的 **pending** 收据；主体不变。

发布/存储：
- `RecordPartPublication :one`：按收据 token 将 pending→published，写 message_id/stored_size/salt/published_at；0 行=租约已失效。
- `AdoptStoredPart :one`（取代 Service.StorePart 对 `MarkUploadPartStored` 的调用）：单语句 CTE——`target_receipt FOR UPDATE`（按 token，**允许 state 为 pending 或 published**，直接调用方与迁移回退兼容）→ `stored` UPDATE upload_parts（要求 part.token=参数 token、part.channel=receipt.channel；salt 用 `COALESCE(receipt.salt, arg.salt)`）→ `adopted` UPDATE 该收据 → RETURNING part；0 行→ErrLeaseLost。
- `CondemnPartReceipt :execrows`：该 token 收据 pending/published→deleting。
- `DeletePartReceipt :exec`：按 (upload,part,token[,message]) 删除，仅在确认远端删除成功后调用。

清理（三类）：
- 改 `ListUploadSessionsPendingCleanup :many`：存在性条件 OR 上 published/deleting 且带 message 的收据。
- `DeleteDeadPendingReceipts :execrows`（尚未发布；不调 Telegram，LIMIT 1000 有界）。
- `ListUploadPartReceiptsForCleanupMany :many` / `DeleteUploadPartReceiptsForCleanup :execrows`：会话 aborted/expired、收据 published/deleting、带双重 NOT EXISTS 守卫（upload_parts、file_parts），JSONB 精确身份删除，模式仿照现有 `DeleteUploadPartsForCleanup`。
- `ListCondemnedReceiptsForCleanup` / `DeleteCondemnedReceiptsForCleanup`：任意会话状态下 state=deleting（含 open）。
- `ListSupersededPublishedReceipts`：part stored 但 message 与收据不同 + 双重守卫（open 会话的重复发布）。
- `DeleteAdoptedReceipts :execrows`：已提交类行级 GC（会话 completed，或对应 part 行已不存在），不调 Telegram。
- 保留 `ListUploadPartsForCleanupMany`、`DeleteUploadPartsForCleanup` 名称与语义（分页 tracer 测试依赖）。

### 3. `internal/uploads/service.go`

保持现有导出方法签名与错误映射；新增：

- 错误 `ErrAdoptionUnavailable`（仅管线内部使用，不加入 mapServiceError）。
- `ClaimPartResult` 增 `Adoption *Adoption{ChannelID, MessageID, StoredSize, Salt}` 字段；`Existing` 语义不变。
- `ClaimPart`：现有逻辑包进**一个 Go 事务**（ClaimUploadPart + InsertPartReceipt 原子）；校验顺序、ErrPartBusy/ErrPartConflict 行为不变。
- `FindTakeablePart(FindTakeableInput)`：Get 校验属主/open/未过期；无行返回 (nil,nil)。
- `ClaimPartForAdoption(ClaimAdoptionInput)`：事务内 LockUploadPart→stored 则返回 Existing→`LockTakeablePartReceipt`（FOR UPDATE 复查）→ validatePartShape + plain_size 精确相等→仅校验收据 channel 所属权（GetChannelForUser，**不**做健康/容量检查，杜绝 rollover 副作用）→ ClaimUploadPartOnReceipt + 删旧收据 + 插入新 published 收据。
- `StorePart`：校验/摘要规范化不变，改调 `AdoptStoredPart`。
- 新增 `RecordPartPublication`、`CondemnPartReceipt`、`DeletePartReceipt` 薄封装。
- `Complete`/`Abort` **不改逻辑**：FOR UPDATE 与 `state IN ('open','completing')` CAS 已保证唯一赢家，completed 重入按 file_id 读回原文件；用并发测试钉住行为。

### 4. `internal/transfer/upload.go`

`UploadCatalog` 接口增加上述 5 个方法；同步更新 [upload_test.go](file:///Users/ding/Documents/swe/0910111/project-04/internal/transfer/upload_test.go) 中两个 fake。

UploadPart 新流程（stored 幂等重读保持在最前，200 行为不变）：
1. **Resolve 之前**先 `FindTakeablePart`；命中则走 `ClaimPartForAdoption`（返回 ErrAdoptionUnavailable 说明竞态失败，回落新发布路径）。
2. 仅新发布路径调用 `channels.Resolve` + `ClaimPart`（已验证显式 channel 的 Resolve 只读；rollover 只在 requested=0）。
3. 租约续期 goroutine 不变（现在同时续 part 与收据）。
4. 新发布路径：加密/读流/Upload 与现状一致 → Upload 成功后**先** `RecordPartPublication`（即便刚丢租约也凭收据 token 落库）→ 再做 stored size、exact.Verify、block hash、checksum 校验。
5. 校验失败（size/body/hash/checksum）：`CondemnPartReceipt`（仅 execrows=1 才继续）→ `deleteUploaded` → 成功则 `DeletePartReceipt`，失败则保留 deleting 交清扫 → `FailPart`（错误码与 join 行为完全不变）。
6. Upload 本身失败：`FailPart("telegram_upload_failed")`（pending 收据被新 CTE 删除），行为不变。
7. `StorePart` 失败的**和解分支（核心修复）**：瞬时错误用同 token 立即重试一次；仍失败则 GetPart 重读——若 part 已 stored 且 (channel,message) 与本次一致，**返回该 part 当成功（原结果获胜，绝不删除）**；其他一切情况（ErrLeaseLost、被异 message 取代、重读失败）保留 published/deleting 收据，**不删任何消息**，返回错误等重试接管。
8. 新接管路径 `adoptPart`：不 Resolve、不 Upload、不生成 salt/cipher；用 exactReader（+按相同 shouldHash 规则挂 hasher）重读客户端正文；stored_size 与 `EncryptedSize(plainSize)`/plainSize 守卫；校验失败同样 condemn+删除+FailPart（复用 checksum_mismatch/body_size_mismatch 码）；通过后 StorePart（salt 由 CTE 从收据复制，block_hashes/checksum 用本次重算值）；返回 201（Existing=false）。

### 5. `internal/jobs/cleanup.go`

`UploadCleanupWorker.Work` 每轮顺序：
1. `ExpireUploadSessions`（不变）。
2. `DeleteDeadPendingReceipts`（无 Telegram 调用）。
3. 会话批次：现有 part 列表 ∪ 收据列表，按 (user,channel) 合并去重，**每频道一次** DeleteMessages；成功后分别执行两个受守卫删除并校验行数；失败即返回 job error，行与会话状态原样保留（只写过期与删除，永不可能写成 completed）。
4. condemned（含 open 会话）+ superseded published 收据：同批删除模式。
5. `DeleteAdoptedReceipts` 行级 GC。
全部为空才退出循环（保持现有 drain 语义与 River MaxAttempts=3 重试）。进程重启后所有状态可从表行重算，12h 周期任务与失败重试自动收敛。

## 测试

集成测试基于 `internal/testutil/postgres`，build tag `integration`，用 `scripts/test-postgres.sh` 运行。

- `internal/uploads/service_integration_test.go`：claim 原子建 pending 收据与忙租约；接管命中/不命中（plain_size 不符→ErrPartConflict、过期会话、已 stored）；StorePart 同事务 adopted；错误 token→ErrLeaseLost；RenewPart 同时续两租约；FailPart 只删 pending；**Abort vs Complete 100 次并发栅格唯一赢家**；Complete 重入返回同一 file。
- `internal/transfer/upload_integration_test.go`（给 memoryStorage/加故障装饰的 catalog）：歧义 StorePart 提交→成功且 deleted 为空、再 PUT 得 200；StorePart 硬错误→收据保留且不删消息；重试**不重新发布**（uploadCount=1、resolver.calls=0）；过期旧 owner 不能覆盖/删除；加密接管复用 salt、密文不变可解密；接管校验失败 condemn+删除；open-ended(-1) 接管。
- `internal/transfer/upload_test.go`：和解三分支（同 message 成功/硬错误不删/异 message 不删）、丢租约后仍记录发布；fake 补齐 5 个新接口方法。
- `internal/jobs/cleanup_integration_test.go`：pending 无 Telegram 删除；expired 会话 published 收据删除；adopted 不产生额外删除；open 会话 deleting 删除；superseded 删除；Telegram 失败行与状态全保留后重试收敛；parts+receipts 单频道单批次；adopted 行 GC。
- 回归保证：零字节、fail/replace/rename、加密、checksum mismatch、普通单次完成相关现有测试不改断言。

## 验证

1. `just generate-db` 后审查 `internal/db/sqlcgen` diff（类型/参数齐全）。
2. `gofmt`（经 `just format`，生成目录除外）、`go vet ./...`、`go test ./...`。
3. `scripts/test-postgres.sh go test -tags=integration ./internal/uploads ./internal/transfer ./internal/jobs`（或 `just test-integration`）。
4. 有条件时 `just test-race` 与 `just check`。

## 明确不做 / 已接受的残余窗口

- Telegram Upload RPC 进行中进程被杀（消息可能已建但无人能写 message_id）：无法在带内消除，仍由 14 天 orphan GC 兜底；断连/取消/DB 歧义等本需求列举的窗口全部由收据闭合。
- 不扩展 `CountChannelStoredMessages`（容量计数）；不向 Abort 请求路径注入远端删除（保持 204 即时返回，删除仍异步）。
- 不下载远端副本做二次哈希校验（与正常路径信任模型一致，代价不成比例）。
- HTTP 契约/UI schema 不变。
