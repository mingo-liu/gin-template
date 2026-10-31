CREATE TABLE IF NOT EXISTS targets (
 target_type TINYINT UNSIGNED NOT NULL,
 target_id BIGINT UNSIGNED NOT NULL,
 title VARCHAR(255) NOT NULL,
 summary TEXT NOT NULL,
 category VARCHAR(64) NOT NULL,
 initialized BOOLEAN NOT NULL DEFAULT FALSE,
 redis_run_id VARCHAR(64) NOT NULL DEFAULT '',
 PRIMARY KEY (target_type, target_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS user_like (
 id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
 user_id BIGINT UNSIGNED NOT NULL,
 target_id BIGINT UNSIGNED NOT NULL,
 target_type TINYINT UNSIGNED NOT NULL,
 liked TINYINT UNSIGNED NOT NULL DEFAULT 0,
 version BIGINT UNSIGNED NOT NULL DEFAULT 0,
 created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
 UNIQUE KEY uk_user_target (user_id, target_type, target_id),
 KEY idx_target (target_type, target_id, liked)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS like_count (
 target_id BIGINT UNSIGNED NOT NULL,
 target_type TINYINT UNSIGNED NOT NULL,
 like_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
 updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
 PRIMARY KEY (target_type, target_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO targets (target_type,target_id,title,summary,category) VALUES
 (1,2001,'把复杂留给系统，把简单留给用户','一次点赞背后，是状态、事件与数据之间的默契协作。探索一个可靠交互是如何诞生的。','工程实践'),
 (1,2002,'慢下来，发现日常里的设计','好的设计往往藏在细节中：恰到好处的留白、自然的反馈，以及每一次被认真对待的点击。','设计观察'),
 (1,2003,'从一条消息开始，理解最终一致性','当数据穿过缓存、消息队列和数据库，如何让重复与乱序成为可以从容应对的日常。','架构笔记');
INSERT IGNORE INTO like_count (target_type,target_id) SELECT target_type,target_id FROM targets WHERE initialized=FALSE;
