# Tests

## 개요

`tests/stream_roundtrip_validation.py`는 별도 테스트 TOML 하나로 다음 검증을 자동화합니다.

- `BINEX -> RTCM` 변환 실행
- `RTCM -> BINEX` 변환 실행
- 각 출력 file 로그에서 내부 `RINEX 3.05` 산출
- 각 출력 file 로그를 `RINGO`로 다시 `RINEX 3.05`로 변환
- 내부 `RINEX`와 외부 `RINEX`를 비교해 `epoch 수`, `satellite row 수`, `non-empty signal field 수`, constellation 집합을 검증
- `BINEX` 출력 비교에서는 `RINGO`가 내보내는 공통 observation type subset 기준으로 `non-empty signal field 수`를 비교
- `OBS/NAV` 헤더와 데이터 라인의 고정열 배치를 검증

## 사전 준비

- Python `3.11+`
- 저장소 루트에서 실행
- `stream_roundtrip_validation.py`가 시작 직후 `binex2rtcm` 패키지를 import하므로 먼저 editable install이 필요

macOS/Linux:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

참고:

- 테스트 스크립트는 생성한 app config를 실행할 때 내부적으로 `src`를 `PYTHONPATH`에 추가하지만, 테스트 스크립트 자신의 import를 위해서는 먼저 설치가 끝나 있어야 합니다.
- `tests/stream_roundtrip_validation.example.toml`의 기본값은 `sample/CHUL_20260327_120000.bnx`, `sample/CHUL_20260327_120000.rtcm3`를 사용하는 오프라인 예제입니다.
- 예시의 `ringo_binary` 경로는 macOS Apple Silicon 기준입니다. Windows, Linux, 다른 경로에서는 환경에 맞게 수정해야 합니다.

## 파일

- `stream_roundtrip_validation.py`
  - 테스트 config를 읽고 임시 메인 app config 2개를 생성합니다.
  - `BINEX -> RTCM`, `RTCM -> BINEX`를 순차 실행합니다.
  - 출력 artifact만 내부 `RINEX`와 `RINGO` 외부 `RINEX`를 비교합니다.
- `stream_roundtrip_validation.example.toml`
  - 테스트 전용 예시 설정입니다.

## 테스트 Config

### `[runtime]`

- `duration_s`
  - 각 방향 변환을 몇 초 동안 실행할지 지정합니다.

### `[tools]`

- `ringo_binary`
  - `ringo` 실행 파일 경로입니다.
  - 예시 값은 `tools/ringo/ringo-v0.9.4-macos_arm64/ringo`입니다.
  - 현재 OS/아키텍처에 맞는 실행 파일 경로로 수정해야 합니다.
- `approx_time`
  - RTCM을 `RINEX 3.05`로 바꿀 때 사용할 기준 시각입니다.
  - RTCM 입력 검증 시 필수입니다.
  - 형식은 `YYYY/MM/DD HH:MM:SS`입니다.

### `[artifacts]`

- `workdir`
  - 테스트 산출물을 저장할 루트 디렉터리입니다.
  - 실제 실행 시에는 그 아래에 timestamp 하위 디렉터리가 생성됩니다.

### `[checks]`

- `min_epoch_count`
- `require_matching_epoch_count`
- `require_matching_satellite_rows`
- `require_matching_signal_fields`
- `require_same_constellations`
- `validate_obs_layout`
- `validate_nav_layout`

### `[binex_input]`, `[rtcm_input]`

앱의 `InputConfig`와 유사한 입력 스펙입니다.

- 지원 `kind`
  - `ntrip_client`
  - `tcp_client`
  - `file_replay`
- 공통 키
  - `name`
  - `session`
  - `chunk_size`
  - `connect_timeout_s`
  - `reconnect_delay_s`
- `ntrip_client`
  - `host`
  - `port`
  - `mountpoint`
  - `username`
  - `password`
  - `send_nmea_gga`
  - `gga_interval_s`
  - `source_position_llh`
- `tcp_client`
  - `host`
  - `port`
- `file_replay`
  - `path`
  - `replay_rate`

## 실행

```bash
.venv/bin/python tests/stream_roundtrip_validation.py \
  --config tests/stream_roundtrip_validation.example.toml
```

Windows PowerShell:

```powershell
.\.venv\Scripts\python.exe tests/stream_roundtrip_validation.py `
  --config tests/stream_roundtrip_validation.example.toml
```

### CHUL 12시 샘플로 오프라인 검증

현재 작업 폴더 기준 로컬 샘플:

- BINEX: `sample/CHUL_20260327_120000.bnx`
- RTCM: `sample/CHUL_20260327_120000.rtcm3`
- 대응 OBS 첫 epoch: `2026-03-27 12:00:00 GPST`

`tests/stream_roundtrip_validation.example.toml`은 기본값이 이미 `file_replay` 입력이므로 그대로 실행해도 됩니다.

```toml
[tools]
approx_time = "2026/03/27 12:00:00"

[binex_input]
name = "CHUL BINEX sample"
session = "BINEX"
kind = "file_replay"
path = "sample/CHUL_20260327_120000.bnx"
replay_rate = 0

[rtcm_input]
name = "CHUL RTCM sample"
session = "RTCM"
kind = "file_replay"
path = "sample/CHUL_20260327_120000.rtcm3"
replay_rate = 0
```

샘플 참고:

- `file_replay`에서는 `host`, `port`, `username`, `password`, `connect_timeout_s`, `reconnect_delay_s`가 필요하지 않습니다.
- `approx_time`은 RTCM 샘플의 관측 시각 근처로 맞춰 두는 편이 안전합니다. 위 CHUL 샘플은 `2026-03-27 12:00:00 GPST`부터 시작합니다.
- 위 `sample/...` 경로는 현재 작업 폴더에 준비된 로컬 샘플 기준입니다. 다른 환경에서는 해당 파일 경로만 바꿔 사용하면 됩니다.

## 산출물

실행이 끝나면 `workdir/<timestamp>/` 아래에 다음이 생성됩니다.

- `generated-configs/`
  - 자동 생성된 메인 app config 2개
- `binex-to-rtcm/`
  - 입력 capture BINEX, 출력 RTCM, 출력 기준 내부 RINEX, 외부 `ringo` 결과
- `rtcm-to-binex/`
  - 입력 capture RTCM, 출력 BINEX, 출력 기준 내부 RINEX, 외부 `ringo` 결과

## 출력 리포트

방향별로 다음 항목을 출력합니다.

- 내부 `RINEX OBS` 경로
- 외부 `RINEX OBS` 경로
- `epoch 수`
- `satellite row 수`
- `non-empty signal field 수`
- constellation 집합
- `NAV` 파일 존재 여부
- 헤더 및 라인 배치 검증 이슈 목록

최종 요약에는 전체 비교 개수, 통과 개수, 실패 개수, 실패한 비교 목록이 포함됩니다.

## 자주 겪는 문제

- `ModuleNotFoundError: No module named 'binex2rtcm'`
  - 저장소 루트에서 `python -m pip install -e ".[dev]"`를 먼저 실행하십시오.
- `ringo binary not found`
  - `[tools].ringo_binary`를 현재 환경의 실행 파일 경로로 수정하거나 `ringo`를 `PATH`에서 찾을 수 있게 설정하십시오.
- `RTCM validation requires [tools].approx_time`
  - RTCM 입력을 검증하려면 `approx_time = "YYYY/MM/DD HH:MM:SS"`를 반드시 지정해야 합니다.
- 인증 실패 또는 연결 실패
  - 예제 파일의 플레이스홀더를 실제 계정으로 바꿨는지, 방화벽/포트 접근이 가능한지 확인하십시오.
