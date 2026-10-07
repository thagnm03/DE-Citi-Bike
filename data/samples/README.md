# Sample Data Manifest

## Purpose

Các file trong thư mục này phục vụ development, data profiling và integration fixtures. GBFS responses được giữ nguyên để có thể kiểm tra schema, duplicate và timestamp anomalies.

## Sources

| Local file | Source |
|---|---|
| `gbfs/system_information.json` | `https://gbfs.lyft.com/gbfs/1.1/bkn/en/system_information.json` |
| `gbfs/station_information.json` | `https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_information.json` |
| `gbfs/station_status_01.json` | `https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_status.json` |
| `gbfs/station_status_02.json` | Same endpoint, later poll |
| `gbfs/station_status_03.json` | Same endpoint, later poll |
| `trips/202604-citibike-tripdata.zip` | `https://s3.amazonaws.com/tripdata/202604-citibike-tripdata.zip` |
| `trips/202604-trip-sample-500.csv` | First 500 rows generated from the archive by `scripts/profile_step2.py` |

Official discovery source:

```text
https://gbfs.citibikenyc.com/gbfs/gbfs.json
```

## SHA-256

| File | Bytes | SHA-256 |
|---|---:|---|
| `gbfs/system_information.json` | 312 | `246B2D91332778D80FDA5695688B596D79AD5AF5CE73D7F644E8208A692C2D1F` |
| `gbfs/station_information.json` | 1,362,572 | `1D1ADDD50AF20E6D491056E9DCE9AB2D361DE0ACFE5FEE9B29D12DC8D094F0C2` |
| `gbfs/station_status_01.json` | 967,572 | `752A1FF8F9EA3435759FB974A581A564E5571F266CBCE8890A5BA424BBAAD329` |
| `gbfs/station_status_02.json` | 967,696 | `7BD49FD9FE47612E365E80ED8B7AD251FF425F06AF74EBA0BAAD893D2D9881B6` |
| `gbfs/station_status_03.json` | 967,696 | `7BD49FD9FE47612E365E80ED8B7AD251FF425F06AF74EBA0BAAD893D2D9881B6` |
| `trips/202604-citibike-tripdata.zip` | 164,592,563 | `FEFC1FCA369818BAEB9206792E02A098B71B59F263064B6627B42317965E2FFB` |
| `trips/202604-trip-sample-500.csv` | 97,115 | `BE6C30C9C9AAE9FFE312F4DEC664C4175B30052E5680A0F0DBABD759F12AA15F` |

`station_status_02.json` và `station_status_03.json` có cùng hash vì được poll trong cùng TTL window.

## Reproduce profile

```powershell
python scripts\profile_step2.py
```

Output:

```text
data/profile-step2.json
```

## Storage note

Monthly ZIP được tải riêng theo hướng dẫn trong `docs/data-pipelines.md` và không được commit. Repository giữ sample CSV nhỏ cùng source/checksum manifest để tái lập việc xử lý.

