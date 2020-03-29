import re

text = """@mayunkj: What a busy year for @MSIntune and #Con123figMgr! From "COBO" to Win32 apps to FileVault, hit milestones… https://t.co/EhdX03wnBg"""
print(re.findall(r"#(\w+)", text))
