# Gin Template

这是一个基于 Go 的 Web 项目初始化模板，用于作为后续新项目的基础目录结构。项目按职责划分后端代码，并预留前端代码、文档、脚本、配置和部署目录。

当前模板的 Go 模块名为 `gin-template`，要求 Go 1.26.0 或更高版本。Go 文件仅包含对应的包名声明，尚未添加业务实现或框架依赖。配置和部署文件均为空占位文件，可在新项目中按需补充。

## 项目架构

```text
gin-template/
├── cmd/
│   └── server/
│       └── main.go                  # 服务启动入口
├── internal/                        # 项目内部代码
│   ├── config/
│   │   └── config.go                # 配置加载
│   ├── handler/
│   │   └── handler.go               # HTTP 请求处理
│   ├── service/
│   │   └── service.go               # 业务逻辑
│   ├── repository/                  # 数据访问
│   │   ├── mysql/
│   │   │   └── repository.go        # MySQL 数据访问
│   │   └── redis/
│   │       └── repository.go        # Redis 数据访问
│   ├── model/
│   │   └── model.go                 # 数据模型
│   ├── middleware/
│   │   └── middleware.go            # HTTP 中间件
│   └── routes/
│       └── routes.go                # 路由注册
├── pkg/                             # 可供其他项目导入的公共代码
│   └── .gitkeep
├── web/                             # 前端代码
│   └── .gitkeep
├── docs/                            # 项目文档
│   └── .gitkeep
├── scripts/                         # 开发、构建和运维脚本
│   └── .gitkeep
├── migrations/                      # 数据库迁移脚本
│   └── .gitkeep
├── configs/
│   └── config.yaml                 # 配置文件占位
├── deploy/
│   └── docker-compose.yml          # 部署编排文件占位
├── .gitignore                       # Git 忽略规则
├── go.mod                           # Go 模块声明
├── go.sum                           # 依赖校验文件
├── Makefile                         # 常用命令占位
└── README.md                        # 项目说明
```

后端按 `routes → handler → service → repository` 划分职责：路由层注册接口，处理层解析请求并组织响应，服务层处理业务逻辑，数据访问层负责持久化和缓存。`model` 用于定义数据模型，`middleware` 用于放置通用请求处理逻辑。

空目录通过 `.gitkeep` 占位，确保 Git 克隆后保留目录结构。添加实际文件后，可以移除对应的 `.gitkeep`。
