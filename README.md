## 环境配置（使用 uv）

简洁说明：全部步骤均使用 `uv` 管理虚拟环境与安装依赖。先确保系统有 Python（>=3.10）并能使用 pip 安装包。

### 前提
- Python >= 3.10 已安装并在 PATH 中可用
- 安装 `uv`（任选其一，只需安装一次）:

```powershell
pip install --user uv
```

（macOS 可在终端使用相同命令）

### Windows（PowerShell）

1. 在项目根创建虚拟环境：

```powershell
uv venv .venv
```

2. 激活环境：

```powershell
.\.venv\Scripts\Activate.ps1
# 或
.\.venv\Scripts\activate
```

3. 使用 uv 安装项目依赖：

```powershell
uv pip install -r requirements.txt
```

4. 验证：

```powershell
uv pip list
.\.venv\Scripts\python.exe --version
```

### macOS（bash / zsh）

1. 在项目根创建虚拟环境：

```bash
uv venv .venv
```

2. 激活环境：

```bash
source .venv/bin/activate
```

3. 使用 uv 安装项目依赖：

```bash
uv pip install -r requirements.txt
```

4. 验证：

```bash
uv pip list
python --version
```
