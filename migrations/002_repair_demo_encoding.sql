-- Repair only known demo text imported as latin1 by the original migration.
-- Correct text, customized content, and all like state remain unchanged.
SET NAMES utf8mb4;

START TRANSACTION;

UPDATE targets AS t
JOIN (
 SELECT 1 AS target_type, 2001 AS target_id,
  '把复杂留给系统，把简单留给用户' AS title,
  '一次点赞背后，是状态、事件与数据之间的默契协作。探索一个可靠交互是如何诞生的。' AS summary,
  '工程实践' AS category
 UNION ALL
 SELECT 1, 2002,
  '慢下来，发现日常里的设计',
  '好的设计往往藏在细节中：恰到好处的留白、自然的反馈，以及每一次被认真对待的点击。',
  '设计观察'
 UNION ALL
 SELECT 1, 2003,
  '从一条消息开始，理解最终一致性',
  '当数据穿过缓存、消息队列和数据库，如何让重复与乱序成为可以从容应对的日常。',
  '架构笔记'
) AS seed ON t.target_type=seed.target_type AND t.target_id=seed.target_id
SET t.title=IF(
 CAST(t.title AS BINARY)=CAST(CONVERT(CONVERT(CAST(seed.title AS BINARY) USING latin1) USING utf8mb4) AS BINARY),
 seed.title,t.title),
 t.summary=IF(
 CAST(t.summary AS BINARY)=CAST(CONVERT(CONVERT(CAST(seed.summary AS BINARY) USING latin1) USING utf8mb4) AS BINARY),
 seed.summary,t.summary),
 t.category=IF(
 CAST(t.category AS BINARY)=CAST(CONVERT(CONVERT(CAST(seed.category AS BINARY) USING latin1) USING utf8mb4) AS BINARY),
 seed.category,t.category);

COMMIT;
