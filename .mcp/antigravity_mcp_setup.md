# Antigravity MCP 서버 설정 가이드

## 설정 방법

1. **VS Code 설정 열기**
   - `Ctrl + ,` 또는 `File > Preferences > Settings`
   - 우측 상단 `{}` 아이콘 클릭 (settings.json 열기)

2. **다음 내용 추가**

```json
{
  "antigravity.mcp.servers": {
    "firebase": {
      "command": "npx",
      "args": ["-y", "@anthropic/firebase-mcp"],
      "env": {
        "GOOGLE_APPLICATION_CREDENTIALS": "C:\\Users\\09153\\Documents\\NawabariGolf-main\\service-account.json"
      }
    },
    "cloud-run": {
      "command": "npx",
      "args": ["-y", "@anthropic/gcp-cloud-run-mcp"],
      "env": {
        "GOOGLE_CLOUD_PROJECT": "golf-ai-480805",
        "GOOGLE_CLOUD_REGION": "asia-northeast3"
      }
    },
    "cloud-logging": {
      "command": "npx",
      "args": ["-y", "@anthropic/gcp-logging-mcp"],
      "env": {
        "GOOGLE_CLOUD_PROJECT": "golf-ai-480805"
      }
    },
    "github": {
      "command": "npx",
      "args": ["-y", "@anthropic/github-mcp"],
      "env": {
        "GITHUB_TOKEN": "YOUR_GITHUB_TOKEN_HERE"
      }
    }
  }
}
```

## GitHub Token 생성 (선택사항)

GitHub MCP를 사용하려면:

1. https://github.com/settings/tokens 접속
2. **Generate new token (classic)** 클릭
3. 권한 선택:
   - `repo` (전체)
   - `workflow`
   - `read:org`
4. 생성된 토큰을 위 설정의 `YOUR_GITHUB_TOKEN_HERE`에 붙여넣기

## 설정 후 VS Code 재시작

설정 저장 후 VS Code를 재시작하면 MCP 서버들이 활성화됩니다.

## 확인 방법

Antigravity 채팅에서:
- "Firestore에 있는 tee_times 컬렉션 확인해줘"
- "ingest-job 마지막 실행 로그 보여줘"

와 같이 명령하면 MCP 서버를 통해 직접 데이터를 조회할 수 있습니다.
