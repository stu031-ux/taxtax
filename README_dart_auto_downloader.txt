
DART 자동 다운로더 (회사 검색 → 선택 → 연도별 보고서 다운로드)
============================================================

### 사용 방법
1) 필요한 패키지 설치
```bash
pip install requests pandas openpyxl
```

2) 실행
```bash
python dart_auto_downloader.py
```

3) 흐름
- API Key 입력 → 회사명 검색(부분 일치) → 결과 번호 선택 → 연도 입력
- 해당 연도 공시 목록 수집(list.json)
- 각 공시의 document.xml ZIP 자동 다운로드
- 요약(엑셀 + CSV) 저장

### 출력
```
DART_<연도>_<회사명>_<corp_code>_ZIP/
 ├─ <rcept_no>.zip
 ├─ 공시ZIP요약_<연도>.xlsx
 └─ 공시ZIP요약_<연도>.csv
```

※ ZIP 파일명은 실제 저장은 `<rcept_no>.zip`이며, 요약표에서 사람이 보기 좋은 `<제출일>_<보고서명>.zip` 형태를 함께 제공합니다.
