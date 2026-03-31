# binex2rtcm

`binex2rtcm`은 GNSS 실시간 스트림과 로그 파일을 `BINEX <-> RTCM` 조합으로 변환하는 Python 기반 transcoder입니다.  
공식 배포 인터페이스는 `CLI + TOML 설정`이며, 내부 Python 모듈은 구현 세부사항으로 간주합니다.

## 개요

- 입력: `ntrip_client`, `tcp_client`, `file_replay`
- 출력: `tcp_server`, `tcp_client`, `file`
- 포맷: `binex`, `rtcm`
- 세션 단위 비동기 처리
- GPST 기준 로그 분기와 파일명 타임스탬프
- 입력/출력 로그의 내부 RINEX OBS/NAV 자동 생성과 선택적 OBS CRX 변환
- 선택 의존성 `pyrtcm` 기반 RTCM self-check
- `RINGO` 기반 외부 RINEX 비교 스크립트
- 콘솔 실시간 모니터

## 지원 매트릭스

| 입력 `data_format` | 출력 `data_format` | 지원 상태 | 비고 |
| --- | --- | --- | --- |
| `binex` | `rtcm` | 지원 | 기본 운영 경로 |
| `binex` | `binex` | 지원 | raw passthrough가 아니라 지원 subset 기준 transcode |
| `rtcm` | `rtcm` | 지원 | 지원 메시지 범위 내 transcode |
| `rtcm` | `binex` | 지원 | 같은 GPST epoch의 MSM들을 혼합 BINEX epoch로 병합 후 기록 |

중요 사항:

- `BINEX <-> BINEX`, `RTCM <-> RTCM`도 bit-exact 복제가 아니라 현재 프로젝트가 해석하는 normalized subset 기준 재인코딩입니다.
- 따라서 vendor-specific 확장 레코드, 미지원 메시지, 미지원 신호는 그대로 보존되지 않습니다.

## 프로토콜 지원 범위

### BINEX 입력

- site metadata: `0x00`
- navigation: `0x01-01` GPS, `0x01-02` GLONASS, `0x01-03` SBAS, `0x01-04` Galileo, `0x01-05` BeiDou, `0x01-06` QZSS
- navigation alias: `0x01-14` upgraded Galileo ephemeris
- receiver state: `0x7D-00`
- observation: `0x7F-05`

### BINEX 출력

- site metadata: `0x00`
- navigation: `0x01-01` GPS, `0x01-02` GLONASS, `0x01-03` SBAS, `0x01-04` Galileo, `0x01-05` BeiDou, `0x01-06` QZSS
- observation: `0x7F-05`

### RTCM 입력 / 출력

- station metadata: `1006`, `1008`, `1033`
- GLONASS code-phase bias: `1230`
- broadcast ephemeris: `1019`, `1020`, `1042`, `1044`, `1045`
- observations: `MSM4`, `MSM5`, `MSM6`, `MSM7`

## 설치

요구 사항:

- Python `3.11+`
- macOS, Linux, Windows에서 실행 가능

런타임 설치:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install .
```

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install .
```

개발/검증 도구 포함 설치:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

참고:

- `parse_with_pyrtcm = true`가 실제로 RTCM self-check를 수행하려면 `pyrtcm`이 설치되어 있어야 합니다.
- `pyrtcm`이 없으면 프로그램은 계속 실행되지만 INFO 로그를 남기고 self-check를 비활성화합니다.
- self-check가 필요하면 `python -m pip install -e ".[dev]"` 또는 `python -m pip install pyrtcm`을 사용하십시오.
- OBS를 `.crx`로 함께 생성하려면 `tools/RNX2CRX`, `tools/rnx2crx` 또는 `PATH`에서 찾을 수 있는 `RNX2CRX` 계열 실행 파일이 필요합니다.

## 빠른 시작

1. 기본 오프라인 예제를 바로 실행합니다.

```bash
binex2rtcm --config config/example.toml
```

모니터와 시간 제한을 함께 쓰려면:

```bash
binex2rtcm --config config/example.toml --monitor --duration 60
```

빠른 시작 참고:

- `config/example.toml`의 기본값은 `sample/CHUL_20260327_120000.bnx`를 재생하는 오프라인 예제입니다.
- 산출물은 `runs/example/` 아래에 생성됩니다.
- 실시간 NTRIP 운영용으로 쓰려면 `config/example.toml`을 `config/local.toml` 같은 로컬 설정 파일로 복사해 `ntrip_client` 입력 블록으로 바꿔 사용하십시오.
- TOML 안의 상대 경로는 `설정 파일 위치`가 아니라 `명령을 실행한 현재 작업 디렉터리` 기준으로 해석됩니다.

### 로컬 샘플 파일로 오프라인 재생

현재 작업 폴더에는 다음 로컬 샘플이 있습니다.

- BINEX: `sample/CHUL_20260327_120000.bnx` (약 `980 KB`, 첫 epoch `2026-03-27 12:00:00 GPST`)
- RTCM: `sample/CHUL_20260327_120000.rtcm3` (약 `852 KB`, 같은 구간을 변환한 결과)

예를 들어 BINEX 샘플 하나만 빠르게 재생해 보려면 다음과 같이 `file_replay` 입력을 만들면 됩니다.

```toml
[[inputs]]
name = "CHUL sample"
session = "sample"
kind = "file_replay"
data_format = "binex"
path = "sample/CHUL_20260327_120000.bnx"
replay_rate = 0

[[outputs]]
name = "sample-log"
session = "sample"
kind = "file"
data_format = "rtcm"
path = "runs/sample/chul.rtcm3"
rinex = { enabled = true, observation = true, navigation = true, crx = false }
```

샘플 참고:

- `replay_rate = 0`이면 가능한 한 빨리 EOF까지 재생합니다.
- `interval`을 생략하면 종료 시점까지 하나의 세그먼트 파일만 유지합니다.
- 위 `sample/...` 경로는 현재 작업 폴더에 준비된 로컬 샘플 기준입니다. 다른 환경에서는 같은 형식의 파일 경로로 바꿔 사용하면 됩니다.

## CLI

```bash
binex2rtcm --config config/example.toml
binex2rtcm --config config/example.toml --monitor
binex2rtcm --config config/example.toml --no-monitor
binex2rtcm --config config/example.toml --duration 30
binex2rtcm --clear-runs
binex2rtcm --clear-runs --runs-dir runs
```

- `--config`: TOML 설정 파일 경로
- `--monitor`: 콘솔 모니터 강제 활성화
- `--no-monitor`: 설정 파일에서 모니터가 켜져 있어도 강제로 비활성화
- `--duration`: 지정한 초 후 자동 종료
- `--clear-runs`: 대상 폴더를 비우고 다시 생성
- `--runs-dir`: `--clear-runs` 대상 경로, 기본값은 `runs`

## 설정

예제 설정은 `config/example.toml`에 포함되어 있습니다.

### 공통 섹션

| 섹션 | 키 | 설명 |
| --- | --- | --- |
| `[logging]` | `level` | 로그 레벨 |
| `[validation]` | `parse_with_pyrtcm` | `pyrtcm` 설치 시 생성 RTCM을 즉시 재파싱해 형식 오류를 점검 |
| `[runtime]` | `duration_s` | 실행 제한 시간, `0` 또는 음수면 무기한 |
| `[monitor]` | `enabled`, `interval_s` | 콘솔 모니터 사용 여부와 갱신 주기 |
| `[scheduler]` | `metadata_interval_s`, `ephemeris_interval_s`, `emit_ephemeris_on_change`, `emit_metadata_on_start` | metadata, ephemeris, observation 재송출 정책 |
| `[scheduler.msm_level_by_system]` | `GPS`, `GLO`, `GAL`, `BDS`, `QZS`, `IRN`, `SBS` | constellation별 MSM 레벨 |

공통 참고:

- `parse_with_pyrtcm = true`여도 `pyrtcm`이 설치되지 않으면 INFO 로그 후 검증이 비활성화됩니다.
- TOML의 상대 경로는 현재 작업 디렉터리 기준입니다.

### 입력 배열 `[[inputs]]`

| 키 | 설명 | 적용 |
| --- | --- | --- |
| `name` | 입력 이름, 전체 설정에서 고유 | 전체 |
| `session` | 세션 이름 | 다중 세션 시 사실상 필수 |
| `kind` | `ntrip_client`, `tcp_client`, `file_replay` | 전체 |
| `data_format` | `binex` 또는 `rtcm` | 전체 |
| `chunk_size` | read 단위 바이트 수 | 전체 |
| `connect_timeout_s` | 연결 타임아웃 | 네트워크 입력 |
| `reconnect_delay_s` | 재접속 대기 시간 | 네트워크 입력 |
| `capture_path` | 입력 원본 로그 base path | 선택 |
| `capture_interval` | 입력 로그 분기 주기 (`5M`, `10M`, `15M`, `30M`, `1H`, `24H`) | `capture_path` 사용 시 선택 |
| `capture_rinex` | 입력 로그 세그먼트 종료 시 내부 RINEX 생성 옵션 (`enabled`, `observation`, `navigation`, `crx`) | `capture_path` 사용 시 선택 |
| `send_nmea_gga` | NTRIP caster에 GGA 전송 여부 | `ntrip_client` |
| `gga_interval_s` | GGA 전송 주기 | `ntrip_client` |
| `source_position_llh` | GGA용 기준 위치 | `send_nmea_gga = true`일 때 필요 |
| `host`, `port` | 네트워크 주소 | `ntrip_client`, `tcp_client` |
| `mountpoint`, `username`, `password` | NTRIP 접속 정보 | `ntrip_client` |
| `path`, `replay_rate` | 재생 파일 경로와 속도 | `file_replay` |

설명:

- `capture_path`의 실제 저장 파일명은 첫 관측 epoch의 GPST를 기준으로 `_YYYYMMDD_HHMMSS`가 자동 부가됩니다.
- `capture_path`의 raw 포맷은 해당 입력의 `data_format`을 그대로 따릅니다.
- `capture_rinex.crx = true`이면 observation RINEX `.rnx`를 추가로 `.crx`로 변환합니다. 변환기가 없거나 실패하면 경고만 남기고 `.rnx`는 유지합니다.

### 출력 배열 `[[outputs]]`

| 키 | 설명 | 적용 |
| --- | --- | --- |
| `name` | 출력 이름, 전체 설정에서 고유 | 전체 |
| `session` | 세션 이름 | 다중 세션 시 사실상 필수 |
| `kind` | `file`, `tcp_server`, `tcp_client` | 전체 |
| `data_format` | `binex` 또는 `rtcm` | 전체 |
| `max_queue` | 출력 비동기 큐 크기 | 전체 |
| `path`, `interval` | 파일 경로와 분기 주기 | `file` |
| `host`, `port` | 네트워크 주소 | `tcp_server`, `tcp_client` |
| `rinex` | 파일 출력 세그먼트 종료 시 내부 RINEX 생성 옵션 (`enabled`, `observation`, `navigation`, `crx`) | `file` 전용 |

설명:

- `kind = "file"`와 `data_format = "binex"` 조합도 지원합니다.
- `rinex`는 `file` 출력에만 적용됩니다.
- 파일 출력도 실제 저장 시 `_YYYYMMDD_HHMMSS`가 붙은 세그먼트 파일로 생성됩니다.
- `rinex.crx = true`이면 observation RINEX `.rnx`를 추가로 `.crx`로 변환합니다. navigation RINEX는 항상 `.rnx`를 유지합니다.
- `interval`을 생략하면 종료 시점까지 단일 세그먼트 파일을 유지한 뒤 close합니다.

### 세션 규칙

- 입력과 출력은 `session` 값이 같은 것끼리만 연결됩니다.
- 입력이 2개 이상이면 모든 `[[inputs]]`에 `session`을 명시해야 합니다.
- 세션이 2개 이상이면 모든 `[[outputs]]`에도 `session`을 명시해야 합니다.
- 각 세션은 최소 1개의 입력과 1개의 출력을 가져야 합니다.

## 메타데이터와 로그 처리

### 기준국 정보

- 기준국 좌표, 안테나, 수신기 정보는 TOML에서 직접 받지 않습니다.
- BINEX 입력에서는 `0x00` site metadata를 사용합니다.
- RTCM 입력에서는 `1006`, `1008`, `1033`을 사용합니다.
- RTCM 출력의 `Reference Station ID`는 항상 `0`으로 고정됩니다.

### RTCM 1230

- `1230`은 별도 TOML on/off 없이 metadata 묶음과 함께 항상 송출됩니다.
- 현재 구현은 `bias indicator = 0`, L1/L2 bias mask = 0, bias 값 미포함 상태입니다.
- 즉 구조적으로는 송출되지만 실 bias 값은 비활성/zeroed 상태입니다.

### GPST 기준 파일 분기

- 입력/출력 raw 로그 모두 분기 기준은 로컬 시스템 시간이 아니라 관측에서 복원된 `GPST calendar date/time`입니다.
- 예를 들어 `10M`이면 `11:40:00`, `11:50:00`, `12:00:00` 경계에서 분기합니다.
- 첫 세그먼트 이름은 첫 관측 epoch의 GPST, 이후 세그먼트는 회전 경계 GPST를 사용합니다.
- 파일명 형식은 `YYYYMMDD_HHMMSS`입니다.
- 프로그램 종료 시 마지막 세그먼트도 즉시 close되고 OBS는 `*_MO_YYYYMMDD_HHMMSS.rnx`, NAV는 `*_MN_YYYYMMDD_HHMMSS.rnx` 형식으로 생성됩니다.
- `crx = true`이면 OBS `.rnx`와 함께 같은 basename의 `.crx`도 추가 생성됩니다.

파일명 예:

- 설정 경로: `runs/default/output.rtcm3`
- 실제 세그먼트: `runs/default/output_20260326_114527.rtcm3`
- 내부 OBS RINEX: `runs/default/output_MO_20260326_114527.rnx`
- 내부 NAV RINEX: `runs/default/output_MN_20260326_114527.rnx`
- `crx = true`일 때 추가 OBS CRX: `runs/default/output_MO_20260326_114527.crx`

## 예시

### BINEX NTRIP -> RTCM 서비스

```toml
[[inputs]]
name = "soul-ntrip"
session = "default"
kind = "ntrip_client"
data_format = "binex"
host = "gnssdata.or.kr"
port = 2101
mountpoint = "SOUL-BINEX"
username = "your-id"
password = "your-password"
capture_path = "runs/default/input.bnx"
capture_interval = "10M"
capture_rinex = { enabled = true, observation = true, navigation = true, crx = false }

[[outputs]]
name = "default-server"
session = "default"
kind = "tcp_server"
data_format = "rtcm"
host = "0.0.0.0"
port = 9002

[[outputs]]
name = "default-log"
session = "default"
kind = "file"
data_format = "rtcm"
path = "runs/default/output.rtcm3"
interval = "10M"
rinex = { enabled = true, observation = true, navigation = true, crx = false }
```

### RTCM 파일 재생 -> BINEX 파일

```toml
[[inputs]]
name = "rtcm-replay"
session = "replay"
kind = "file_replay"
data_format = "rtcm"
path = "data/input.rtcm3"
replay_rate = 0

[[outputs]]
name = "binex-log"
session = "replay"
kind = "file"
data_format = "binex"
path = "runs/replay/output.bnx"
interval = "1H"
rinex = { enabled = true, observation = true, navigation = true, crx = false }
```

### 다중 세션

```toml
[[inputs]]
name = "soul-ntrip"
session = "soul"
kind = "ntrip_client"
data_format = "binex"
host = "gnssdata.or.kr"
port = 2101
mountpoint = "SOUL-BINEX"
username = "your-id"
password = "your-password"
capture_path = "runs/soul/input.bnx"
capture_interval = "10M"
capture_rinex = { enabled = true, observation = true, navigation = true, crx = false }

[[outputs]]
name = "soul-server"
session = "soul"
kind = "tcp_server"
data_format = "rtcm"
host = "0.0.0.0"
port = 9002

[[outputs]]
name = "soul-log"
session = "soul"
kind = "file"
data_format = "rtcm"
path = "runs/soul/output.rtcm3"
interval = "10M"
rinex = { enabled = true, observation = true, navigation = true, crx = false }

[[inputs]]
name = "yanj-ntrip"
session = "yanj"
kind = "ntrip_client"
data_format = "binex"
host = "gnssdata.or.kr"
port = 2101
mountpoint = "YANJ-BINEX"
username = "your-id"
password = "your-password"
capture_path = "runs/yanj/input.bnx"
capture_interval = "10M"
capture_rinex = { enabled = true, observation = true, navigation = true, crx = false }

[[outputs]]
name = "yanj-log"
session = "yanj"
kind = "file"
data_format = "binex"
path = "runs/yanj/output.bnx"
interval = "10M"
rinex = { enabled = true, observation = true, navigation = true, crx = false }
```

## 라이브 모니터

모니터를 활성화하면 세션별 입력/출력 상태를 주기적으로 표시합니다.

- 입력 상태: `waiting`, `active`, `quiet`
- 누적 입력 바이트, frame 수, epoch 수, ephemeris 수
- 생성한 RTCM 메시지 수
- 원본 캡처 바이트 수
- 출력 전송 바이트와 오류 상태

```bash
binex2rtcm --config config/example.toml --monitor
```

테스트와 검증 사용법은 `tests/README.md`를 참조하십시오.

## 자주 겪는 문제

- `binex2rtcm: command not found`가 나오면 가상환경을 활성화했는지 확인하거나 `python -m binex2rtcm --config ...` 형태로 실행하십시오.
- `pyrtcm is not installed; RTCM parse validation disabled` 로그가 나오면 `pyrtcm`을 추가 설치해야 self-check가 동작합니다.
- 설정한 파일명과 실제 산출물 파일명이 다르면 GPST 기준 `_YYYYMMDD_HHMMSS` 접미사가 자동으로 붙는 동작인지 먼저 확인하십시오.
- 설정 TOML 문법이 잘못되었거나 경로가 틀리면 `Configuration file not found: ...` 오류로 종료됩니다. 먼저 파일 경로와 TOML 문법을 함께 확인하십시오.
- 상대 경로 입력이 기대와 다르면 명령을 실행한 현재 작업 디렉터리를 기준으로 다시 확인하십시오.
- `split BDS epoch ... into 2 RTCM MSM messages ...`는 오류가 아니라 `INFO` 로그입니다. BDS 관측 한 epoch가 RTCM MSM mask slot 제한(`<= 64`)을 넘어서 여러 메시지로 나뉘었다는 뜻입니다.
- 로그가 몇 초간 조용하다가 같은 초에 여러 줄이 몰려 보여도, 입력이 chunked/buffered 상태로 들어온 뒤 내부 큐가 빠르게 비워진 경우일 수 있습니다. `WARNING`, `ERROR`, `NTRIP reconnect after error:`가 없다면 우선 정상 동작으로 보는 편이 맞습니다.
- `Ctrl+C` 뒤에 split 로그가 한 줄 더 찍히고 `shutdown requested by user`가 나오는 것은 이미 읽어 둔 chunk와 pending epoch를 마저 처리한 뒤 종료하기 때문입니다.

## 운영 권장 사항

- 저장소에는 실제 계정과 비밀번호를 넣지 말고 환경별 TOML을 별도 관리하십시오.
- `config/example.toml`은 예제 전용으로 유지하고, 운영 설정은 `config/*.local.toml` 또는 별도 비추적 파일로 관리하는 편이 안전합니다.
- 하나의 세션에서 서비스와 로그를 동시에 원하면 `tcp_*` 출력과 `file` 출력을 함께 두십시오.
- station ID가 항상 `0`이므로 서로 다른 기준국 세션을 하나의 동일한 RTCM 소비 지점으로 합치는 구성은 피하는 편이 안전합니다.
- `ntrip_client`는 `Transfer-Encoding: chunked` 응답도 처리합니다.

## 제한 사항

- 공식 지원 인터페이스는 CLI와 TOML 설정입니다. 내부 Python API는 안정된 공개 API가 아닙니다.
- `1041` NavIC/IRNSS ephemeris는 아직 구현하지 않았습니다.
- `1043`은 RTCM 3.3 기준 정의된 broadcast ephemeris 메시지가 아니므로 지원하지 않습니다.
- IRNSS MSM signal map은 최소 범위만 포함하며 현재 `5A` 중심입니다.
- 라이브 BINEX 스트림에서 실제로 소비하는 레코드는 `0x00`, `0x01-01..06`, `0x01-14`, `0x7D-00`, `0x7F-05`입니다.
- BINEX 출력은 위 decoded subset만 재구성하며 vendor-specific 추가 레코드는 보존하지 않습니다.
- `0x01-14`는 upgraded Galileo ephemeris로 받아들이며 현재 프로젝트에서는 공통 Galileo decoded layout로 정규화합니다.
- `RTCM 1230`은 현재 disabled/zeroed payload 상태로 metadata와 함께 송출됩니다.

## 라이선스

이 프로젝트는 `BSD-2-Clause` 라이선스를 사용합니다. 자세한 내용은 `LICENSE`를 참조하십시오.
