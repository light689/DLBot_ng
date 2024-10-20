# DLBot_ng

DLBot_ng 是一个基于 WebSocket 的聊天机器人，用于连接到 hack.chat 平台。它能够自动加入指定的聊天频道，并根据预设条件发送和记录消息。

## 目前计划

- [x] 消息日志记录
- [x] 掉线重连
- [x] RL重连
- [x] 附身（$chat）
- [x] 通过配置文件设置用户信息
- [x] WebUI发送消息
- [x] kick 拦截
- [x] WebUI读取消息记录
- [x] async多线程运行
- [x] 私信命令处理（弃用）
- [x] 更名重进
- [x] [CMP](https://cmd.0k.gs)自动RL处理（感谢[cmd1152](https://github.com/cmd1152)）
- [x] 更好的WebUI
- [ ] **反向ws，通过ws连接DLBot以获取&发送消息** I'm making this :D
- [ ] Update_Message支持
- [ ] `$gethistory`读取消息记录
- [ ] 通过配置文件增加命令
- [ ] 更多……

## 功能

- **自动加入频道**：机器人能够自动加入指定的 hack.chat 频道。
- **消息记录**：所有接收和发送的消息都会被记录到日志文件中。
- **自动重试**：当遇到昵称被占用或加入频道过快的情况时，机器人会自动重试。
- **私信处理**：机器人能够处理特定的私信命令，并根据命令内容发送消息。

## 截图

![.png](https://s2.loli.net/2024/09/17/T8nJIh3duremNGQ.png)
![图片](https://github.com/user-attachments/assets/ee469b22-acb0-496c-a089-24464653683e)


## 配置

在运行机器人之前，请确保在 `user.txt` 文件中正确配置以下内容：

1. 用户名 (`username:`)
2. 密码 (`password:`)
3. 频道名称 (`channel:`)
4. 信任用户识别码列表 (`trustedusers:`)
5. WebSocket 链接 (`ws_link:`)

示例 `user.txt` 内容：

```
username: MyNickname
password: MyPassword
channel: MyChannel
trustedusers: ["Trust1", "Trust2"]
ws_link: wss://hack.chat/chat-ws
```

## 日志

- **log.log**：记录所有系统日志和消息日志。
- **msg.log**：仅记录聊天消息。

## 依赖

requirements.txt

## 安装

1. 克隆仓库：
    ```bash
    git clone https://github.com/lightworld689/DLBot_ng.git
    ```

2. 安装依赖：
    ```bash
    pip install -r requirements.txt
    ```

3. 运行机器人：
    ```bash
    python main.py
    ```

## 贡献

欢迎贡献代码或提出改进建议。请通过 GitHub 提交 Issue 或 Pull Request。

## 许可证

本项目采用 AGPL-3.0 许可证。详情请参见 [LICENSE](LICENSE) 文件。
