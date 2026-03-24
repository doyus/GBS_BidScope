# 招投标数据抓取与分析平台

一个功能完整的招投标数据采集、浏览和分析系统，支持多源数据抓取、可视化分析和灵活的数据导出。

## 项目概述

本项目是一个面向招投标信息的数据处理平台，具备以下核心能力：

- **数据采集**：支持多域名招投标网站的自动化数据抓取
- **数据浏览**：提供Web界面浏览和管理抓取的数据
- **数据分析**：可视化仪表盘展示数据统计特征
- **数据导出**：支持按条件筛选导出Excel/CSV/分析报告

## 技术栈

| 组件 | 技术 |
|------|------|
| 后端框架 | Flask 3.0+ |
| 数据库 | SQLite3 |
| 数据处理 | pandas 2.0+ |
| 数据导出 | openpyxl (Excel) |
| 可视化 | ECharts 5.4+ |
| 爬虫框架 | DrissionPage 4.0+ |

## 项目结构

```
gbs_project03/
├── web_viewer.py           # Flask Web应用主入口
├── data_analytics.py       # 数据分析模块
├── crawl_update_content.py # 爬虫主程序
├── crawl_local.db          # SQLite数据库
├── requirements.txt        # 依赖列表
│
└── templates/              # HTML模板
    ├── viewer_base.html        # 基础模板
    ├── viewer_index.html       # 数据列表页
    ├── viewer_detail.html      # 详情页
    ├── viewer_analytics.html   # 数据分析仪表盘
    ├── viewer_export.html      # 数据导出页
    ├── viewer_report.html      # 分析报告模板
    ├── viewer_admin_clear.html # 数据清空页
    └── viewer_error.html       # 错误页
```

## 核心功能

### 1. 数据采集
- 支持多域名招投标网站的自动化抓取
- 智能XPath学习，自动适配不同网站结构
- 抓取状态跟踪和失败重试机制
- URL去重，避免重复抓取

### 2. 数据浏览
- 列表展示所有抓取的数据
- 支持按ID、正文、元数据搜索
- 按抓取状态筛选（全部/成功/失败/重试中）
- 分页展示，支持自定义每页条数
- 详情页查看完整内容和元数据

### 3. 数据分析
- **统计概览**：总记录数、成功率、域名数、时间跨度
- **内容分析**：平均长度、长度分布、中位数统计
- **来源分析**：TOP域名数据量排行
- **趋势分析**：每日抓取量趋势
- **质量报告**：抓取成功率、失败原因分类

### 4. 可视化图表
- 每日抓取趋势折线图
- 各来源网站数据量柱状图
- 内容长度分布饼图
- 抓取状态分布饼图

### 5. 数据导出
- **Excel导出**：支持筛选条件的.xlsx文件
- **CSV导出**：支持筛选条件的.csv文件
- **分析报告**：可打印的HTML格式报告

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

复制示例配置文件：

```bash
cp .env.example .env
```

编辑 `.env` 文件，填写你的智谱AI API Key：

```bash
# 必填：智谱AI API Key (从 https://open.bigmodel.cn/ 获取)
ZHIPU_API_KEY=your_api_key_here

# 可选：Flask密钥（生产环境建议修改）
FLASK_SECRET_KEY=your_secret_key_here
```

或者使用系统环境变量：

```bash
# Linux/Mac
export ZHIPU_API_KEY=your_api_key_here
export FLASK_SECRET_KEY=your_secret_key_here

# Windows PowerShell
$env:ZHIPU_API_KEY="your_api_key_here"
$env:FLASK_SECRET_KEY="your_secret_key_here"
```

### 3. 启动Web服务

```bash
python web_viewer.py
```

访问 http://127.0.0.1:5050

### 4. 运行爬虫

```bash
python crawl_update_content.py
```

## API接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/` | GET | 数据列表页 |
| `/item/<id>` | GET | 数据详情页 |
| `/analytics` | GET | 数据分析仪表盘 |
| `/api/analytics/data` | GET | 完整分析数据JSON |
| `/api/analytics/daily-trend` | GET | 每日趋势数据 |
| `/api/analytics/domain-stats` | GET | 域名统计数据 |
| `/api/analytics/content-length` | GET | 内容长度分布 |
| `/export` | GET | 导出页面 |
| `/export/excel` | GET | 导出Excel |
| `/export/csv` | GET | 导出CSV |
| `/export/report` | GET | 下载分析报告 |

## 数据库结构

### cms_crawl_data_content 表
| 字段 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 主键 |
| description | TEXT | 抓取的正文内容(HTML) |
| updated_at | REAL | 更新时间戳 |
| excel_meta | TEXT | Excel元数据(JSON) |
| crawl_status | TEXT | 抓取状态(ok/failed/retrying) |
| crawl_error | TEXT | 错误信息 |
| crawl_fail_count | INTEGER | 失败次数 |

## 数据分析指标

### 内容统计
- 总记录数
- 平均/中位数/最小/最大内容长度
- 内容长度分布（0-100/100-500/500-1000/1000-5000/5000+）

### 域名统计
- 不同域名数量
- TOP 20 域名数据量排行
- 各域名抓取成功率

### 时间统计
- 数据时间跨度
- 每日数据量趋势
- 近7天/30天新增数据

### 质量报告
- 抓取成功率
- 失败原因分类（超时/网络/HTTP错误/解析错误/内容错误/其他）
- 重试次数分布

## 截图预览

### 数据列表页
- 表格展示所有数据
- 支持搜索和筛选
- 显示抓取状态和字数统计

### 数据分析仪表盘
- 核心指标卡片
- 多种可视化图表
- 数据质量报告

### 数据导出页
- 灵活的筛选条件
- 多种导出格式

## 开发计划

- [x] 数据采集功能
- [x] Web数据浏览
- [x] 数据分析仪表盘
- [x] 数据导出功能
- [ ] 定时任务调度
- [ ] 用户权限管理
- [ ] 数据备份恢复

## 许可证

MIT License

## 作者

项目开发团队
