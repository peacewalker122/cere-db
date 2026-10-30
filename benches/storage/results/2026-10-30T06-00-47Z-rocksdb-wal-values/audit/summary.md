# Final matched KV storage benchmark

Run root: `/home/peacewalker/project/learn/wasm-kv/target/storage-compare-final-20261030T054701Z`
Base commit: `fa65cde5d1642a54a576b89f0f2c88dba8a13fb5`; initial worktree was dirty; no selected source inputs changed during run: **PASS** (66 SHA-256 entries, all captures identical).
Fresh release binary SHA-256: `5b06ca38c67b4dd16a4336fe5b2fe70f196e79c8d2ba6d63f4e962ab3b71f6c3`; before pilot, before full, and after full identical: **PASS**.
Pilot: exit `0`, schema 2, 3 engine rows, `performance_result=false`; functional checks: **PASS**.
Full matrix: exit `0`, schema 2, 96 execution rows (72 measured + 24 warmups), 8 cases × 3 engines; report `performance_result=true`.
Filesystem: `/home` / `btrfs` / `/dev/nvme0n1p3[/home]`; options `rw,relatime,seclabel,compress=zstd:1,ssd,discard=async,space_cache=v2,subvolid=257,subvol=/home`; exact dataset mount identity/options unchanged.
RocksDB: crate `0.24.0`, native `10.4.2`, WAL sync enabled, 1 MiB write buffer, compression disabled.
CereDB: 1 MiB memtable + 1 MiB WAL; other settings `StorageConfig::default()`.

## Per-case results

Times are median `[min–max]` ns across three measured samples; warmups excluded. Rates divide actual timed-loop operation counts by median append/mutation timer only. `Experimental/RocksDB` time ratio below 1 means Experimental was faster; above 1 means slower. KV2 is current-run control, not historical baseline.

### `v256_n1024_b1`

#### Timed writes and natural state

| Engine | Append ns [range] | Append ops/s | Append ops / calls / input B | Mutation ns [range] | Mutation ops/s | Mutation ops / calls / input B | Natural closed root B [range] | B/live byte | Actual final root B [range] |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| KV2 inline | 6,388,670,969 [3,130,754,574–7,163,434,344] | 160.284 | 1,024 / 1,024 / 282,624 | 2,581,072,634 [1,807,556,987–2,908,672,345] | 158.461 | 409 / 409 / 86,772 | 511,299 [511,299–511,299] | 1.903939 | 511,299 [511,299–511,299] |
| Experimental WAL | 6,537,517,079 [3,175,004,680–7,289,381,450] | 156.634 | 1,024 / 1,024 / 282,624 | 2,600,356,432 [1,282,031,507–2,850,211,347] | 157.286 | 409 / 409 / 86,772 | 511,376 [511,376–511,376] | 1.904226 | 475,485 [475,485–475,485] |
| RocksDB | 6,132,561,744 [3,089,755,088–6,837,527,560] | 166.978 | 1,024 / 1,024 / 282,624 | 2,481,011,758 [1,169,280,240–2,583,604,338] | 164.852 | 409 / 409 / 86,772 | 439,449 [439,449–439,449] | 1.636389 | 344,758 [344,758–344,758] |

#### Phase timings

| Phase | KV2 inline | Experimental WAL | RocksDB |
|---|---:|---:|---:|
| fresh open | 70,230,491 [36,581,428–72,228,598] | 113,088,634 [59,259,566–125,228,084] | 62,949,328 [29,874,844–68,207,889] |
| append | 6,388,670,969 [3,130,754,574–7,163,434,344] | 6,537,517,079 [3,175,004,680–7,289,381,450] | 6,132,561,744 [3,089,755,088–6,837,527,560] |
| mutations | 2,581,072,634 [1,807,556,987–2,908,672,345] | 2,600,356,432 [1,282,031,507–2,850,211,347] | 2,481,011,758 [1,169,280,240–2,583,604,338] |
| barrier drain | 2,587 [2,285–2,611] | 2,159 [1,904–2,560] | 5,808,620 [2,676,908–9,722,909] |
| close | 339,573 [307,398–356,291] | 13,184,042 [6,488,675–21,666,605] | 287,449 [220,174–363,276] |
| reopen uncleaned | 25,366,038 [24,213,239–29,959,039] | 10,941,050 [8,518,542–13,938,802] | 41,655,946 [21,055,125–42,115,741] |
| warm get uncleaned | 3,440 [2,710–3,584] | 580,313 [507,756–675,878] | 7,632 [7,379–8,001] |
| warm scan uncleaned | 6,004 [5,888–6,072] | 582,101 [511,311–599,451] | 10,258 [10,211–10,492] |
| clean elapsed | — | 365,576,037 [273,069,404–389,759,884] | — |
| clean write pause | — | 358,067,614 [269,630,670–381,761,915] | — |
| barrier after clean | — | 1,762 [1,142–1,948] | — |
| close after clean | — | 148,370 [45,521–174,669] | — |
| reopen post clean | — | 13,891,268 [11,186,564–14,113,995] | — |
| warm get post clean | — | 820,452 [809,729–914,762] | — |
| warm scan post clean | — | 915,648 [908,263–1,013,516] | — |
| close after verification | 127,194 [118,301–132,297] | — | 219,943 [185,835–230,097] |
| close post clean verification | — | 185,927 [175,776–253,625] | — |
| lifecycle sum | 9,064,625,275 [5,000,753,511–10,174,778,135] | 9,664,122,632 [4,821,339,637–10,698,785,921] | 8,724,554,766 [4,313,177,875–9,541,636,144] |

#### Experimental cleaning and post-clean reads

Natural Experimental root: **511,376 [511,376–511,376] B**; post-clean Experimental root: **475,485 [475,485–475,485] B**; natural RocksDB root: **439,449 [439,449–439,449] B**. These are different lifecycle states; cleaned Experimental bytes are not presented as equivalent to natural RocksDB.

Post-clean Experimental B/live byte: median 1.770577 (range 1.770577–1.770577). Natural Experimental B/live byte: median 1.904226.

| Clean stat | Median [min–max] |
|---|---:|
| Root bytes before | 511,376 [511,376–511,376] |
| Sampled peak bytes | 986,812 [986,812–986,812] |
| Root bytes after | 475,485 [475,485–475,485] |
| Deferred cleanup bytes | 0 [0–0] |
| Live records | 973 [973–973] |
| Live key+value bytes | 268,548 [268,548–268,548] |
| Copied payload bytes | 249,088 [249,088–249,088] |
| Copied transactions | 4 [4–4] |
| Index SSTables written | 1 [1–1] |
| Cleaner write pause ns | 358,067,614 [269,630,670–381,761,915] |
| Cleaner elapsed ns | 365,576,037 [273,069,404–389,759,884] |

Warm reads each sample: natural 8 get calls/8 entries + 1 scan/8 entries; post-clean Experimental same 8/8 + 1/8. Natural reads use `warm_get_uncleaned_ns`/`warm_scan_uncleaned_ns`; post-clean reads use separate timers. Experimental post-clean reopen median/range: 13,891,268 [11,186,564–14,113,995] ns. Cleaner `write_pause_ns` is nested in `elapsed_ns` and is not double-counted in lifecycle sum.

#### Experimental / RocksDB direction

| Timed phase | Experimental/RocksDB median time ratio | Difference (Exp − RocksDB) ns | Direction |
|---|---:|---:|---|
| append | 1.066034× | +404,955,335 | Experimental slower |
| mutations | 1.048103× | +119,344,674 | Experimental slower |
| reopen uncleaned | 0.262653× | -30,714,896 | Experimental faster |
| warm get uncleaned | 76.036819× | +572,681 | Experimental slower |
| warm scan uncleaned | 56.746052× | +571,843 | Experimental slower |
| lifecycle sum | 1.107692× | +939,567,866 | Experimental slower |
| Append throughput | 0.938057× rate | — | higher rate favors Experimental |
| Mutation throughput | 0.954104× rate | — | higher rate favors Experimental |

Footprint ratios: natural Experimental / natural RocksDB = 1.163675×; post-clean Experimental / natural RocksDB = 1.082003× (non-equivalent lifecycle stages).

Observed level counts and evidence:
- KV2 inline: sample 1: {} (ceredb_public_manifest_snapshot_while_closed); sample 2: {} (ceredb_public_manifest_snapshot_while_closed); sample 3: {} (ceredb_public_manifest_snapshot_while_closed)
- Experimental WAL: sample 1: {} (ceredb_public_manifest_snapshot_while_closed); sample 2: {} (ceredb_public_manifest_snapshot_while_closed); sample 3: {} (ceredb_public_manifest_snapshot_while_closed)
- RocksDB: sample 1: {"0": 1} (rocksdb_live_files_after_reopen); sample 2: {"0": 1} (rocksdb_live_files_after_reopen); sample 3: {"0": 1} (rocksdb_live_files_after_reopen)

### `v256_n1024_b16`

#### Timed writes and natural state

| Engine | Append ns [range] | Append ops/s | Append ops / calls / input B | Mutation ns [range] | Mutation ops/s | Mutation ops / calls / input B | Natural closed root B [range] | B/live byte | Actual final root B [range] |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| KV2 inline | 447,499,980 [440,014,666–454,249,099] | 2,288.268 | 1,024 / 64 / 282,624 | 182,205,776 [176,654,271–189,547,283] | 2,244.715 | 409 / 26 / 86,772 | 461,608 [461,608–461,608] | 1.718903 | 461,608 [461,608–461,608] |
| Experimental WAL | 447,365,866 [441,942,036–465,365,547] | 2,288.954 | 1,024 / 64 / 282,624 | 183,950,378 [178,686,100–191,842,673] | 2,223.426 | 409 / 26 / 86,772 | 461,685 [461,685–461,685] | 1.719190 | 475,486 [475,486–475,486] |
| RocksDB | 430,218,922 [421,836,316–435,040,723] | 2,380.184 | 1,024 / 64 / 282,624 | 178,738,602 [175,683,910–181,677,185] | 2,288.258 | 409 / 26 / 86,772 | 413,935 [413,935–413,935] | 1.541382 | 344,767 [344,767–344,767] |

#### Phase timings

| Phase | KV2 inline | Experimental WAL | RocksDB |
|---|---:|---:|---:|
| fresh open | 80,240,737 [72,094,975–81,794,341] | 130,281,600 [127,420,952–133,372,193] | 67,020,233 [65,543,432–67,754,030] |
| append | 447,499,980 [440,014,666–454,249,099] | 447,365,866 [441,942,036–465,365,547] | 430,218,922 [421,836,316–435,040,723] |
| mutations | 182,205,776 [176,654,271–189,547,283] | 183,950,378 [178,686,100–191,842,673] | 178,738,602 [175,683,910–181,677,185] |
| barrier drain | 2,450 [2,053–2,467] | 2,496 [2,135–2,899] | 3,991 [3,821–4,688] |
| close | 225,738 [215,240–255,303] | 13,266,111 [12,555,947–14,747,387] | 267,877 [234,252–318,087] |
| reopen uncleaned | 25,448,221 [25,421,609–26,503,755] | 13,191,813 [12,627,446–16,929,455] | 43,179,436 [42,703,139–46,759,195] |
| warm get uncleaned | 3,248 [3,129–4,098] | 454,255 [439,033–472,029] | 7,644 [7,337–7,890] |
| warm scan uncleaned | 6,052 [5,986–6,245] | 444,413 [440,531–462,856] | 10,962 [9,974–11,079] |
| clean elapsed | — | 363,814,886 [363,323,394–385,013,022] | — |
| clean write pause | — | 357,115,423 [357,011,628–378,109,623] | — |
| barrier after clean | — | 1,466 [1,452–1,973] | — |
| close after clean | — | 46,556 [40,360–148,167] | — |
| reopen post clean | — | 13,044,256 [12,519,757–14,867,172] | — |
| warm get post clean | — | 791,807 [785,853–827,838] | — |
| warm scan post clean | — | 898,547 [894,378–915,221] | — |
| close after verification | 199,304 [108,091–207,308] | — | 245,499 [188,297–270,229] |
| close post clean verification | — | 159,765 [159,453–185,335] | — |
| lifecycle sum | 730,188,603 [720,171,777–752,561,045] | 1,190,046,358 [1,159,199,286–1,195,461,165] | 716,888,240 [709,834,149–731,024,361] |

#### Experimental cleaning and post-clean reads

Natural Experimental root: **461,685 [461,685–461,685] B**; post-clean Experimental root: **475,486 [475,486–475,486] B**; natural RocksDB root: **413,935 [413,935–413,935] B**. These are different lifecycle states; cleaned Experimental bytes are not presented as equivalent to natural RocksDB.

Post-clean Experimental B/live byte: median 1.770581 (range 1.770581–1.770581). Natural Experimental B/live byte: median 1.719190.

| Clean stat | Median [min–max] |
|---|---:|
| Root bytes before | 461,685 [461,685–461,685] |
| Sampled peak bytes | 937,122 [937,122–937,122] |
| Root bytes after | 475,486 [475,486–475,486] |
| Deferred cleanup bytes | 0 [0–0] |
| Live records | 973 [973–973] |
| Live key+value bytes | 268,548 [268,548–268,548] |
| Copied payload bytes | 249,088 [249,088–249,088] |
| Copied transactions | 4 [4–4] |
| Index SSTables written | 1 [1–1] |
| Cleaner write pause ns | 357,115,423 [357,011,628–378,109,623] |
| Cleaner elapsed ns | 363,814,886 [363,323,394–385,013,022] |

Warm reads each sample: natural 8 get calls/8 entries + 1 scan/8 entries; post-clean Experimental same 8/8 + 1/8. Natural reads use `warm_get_uncleaned_ns`/`warm_scan_uncleaned_ns`; post-clean reads use separate timers. Experimental post-clean reopen median/range: 13,044,256 [12,519,757–14,867,172] ns. Cleaner `write_pause_ns` is nested in `elapsed_ns` and is not double-counted in lifecycle sum.

#### Experimental / RocksDB direction

| Timed phase | Experimental/RocksDB median time ratio | Difference (Exp − RocksDB) ns | Direction |
|---|---:|---:|---|
| append | 1.039856× | +17,146,944 | Experimental slower |
| mutations | 1.029159× | +5,211,776 | Experimental slower |
| reopen uncleaned | 0.305511× | -29,987,623 | Experimental faster |
| warm get uncleaned | 59.426347× | +446,611 | Experimental slower |
| warm scan uncleaned | 40.541233× | +433,451 | Experimental slower |
| lifecycle sum | 1.660017× | +473,158,118 | Experimental slower |
| Append throughput | 0.961671× rate | — | higher rate favors Experimental |
| Mutation throughput | 0.971667× rate | — | higher rate favors Experimental |

Footprint ratios: natural Experimental / natural RocksDB = 1.115356×; post-clean Experimental / natural RocksDB = 1.148697× (non-equivalent lifecycle stages).

Observed level counts and evidence:
- KV2 inline: sample 1: {} (ceredb_public_manifest_snapshot_while_closed); sample 2: {} (ceredb_public_manifest_snapshot_while_closed); sample 3: {} (ceredb_public_manifest_snapshot_while_closed)
- Experimental WAL: sample 1: {} (ceredb_public_manifest_snapshot_while_closed); sample 2: {} (ceredb_public_manifest_snapshot_while_closed); sample 3: {} (ceredb_public_manifest_snapshot_while_closed)
- RocksDB: sample 1: {"0": 1} (rocksdb_live_files_after_reopen); sample 2: {"0": 1} (rocksdb_live_files_after_reopen); sample 3: {"0": 1} (rocksdb_live_files_after_reopen)

### `v256_n1024_b64`

#### Timed writes and natural state

| Engine | Append ns [range] | Append ops/s | Append ops / calls / input B | Mutation ns [range] | Mutation ops/s | Mutation ops / calls / input B | Natural closed root B [range] | B/live byte | Actual final root B [range] |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| KV2 inline | 116,462,268 [113,779,236–116,482,962] | 8,792.547 | 1,024 / 16 / 282,624 | 49,491,559 [49,053,117–51,716,945] | 8,264.035 | 409 / 7 / 86,772 | 459,129 [459,129–459,129] | 1.709672 | 459,129 [459,129–459,129] |
| Experimental WAL | 118,030,868 [115,697,202–119,702,349] | 8,675.697 | 1,024 / 16 / 282,624 | 48,879,483 [46,001,279–51,561,097] | 8,367.519 | 409 / 7 / 86,772 | 459,206 [459,206–459,206] | 1.709959 | 475,486 [475,486–475,486] |
| RocksDB | 111,355,244 [107,473,085–123,474,086] | 9,195.795 | 1,024 / 16 / 282,624 | 49,090,758 [44,762,568–50,753,857] | 8,331.507 | 409 / 7 / 86,772 | 412,662 [412,662–412,662] | 1.536641 | 344,767 [344,767–344,767] |

#### Phase timings

| Phase | KV2 inline | Experimental WAL | RocksDB |
|---|---:|---:|---:|
| fresh open | 73,492,084 [72,046,812–76,804,770] | 126,183,603 [124,301,639–133,138,262] | 66,803,829 [66,572,990–69,458,415] |
| append | 116,462,268 [113,779,236–116,482,962] | 118,030,868 [115,697,202–119,702,349] | 111,355,244 [107,473,085–123,474,086] |
| mutations | 49,491,559 [49,053,117–51,716,945] | 48,879,483 [46,001,279–51,561,097] | 49,090,758 [44,762,568–50,753,857] |
| barrier drain | 2,512 [2,202–2,652] | 3,044 [2,440–9,924] | 3,782 [3,668–3,787] |
| close | 162,615 [143,296–190,909] | 13,889,109 [10,935,392–14,653,003] | 253,808 [208,128–365,020] |
| reopen uncleaned | 24,583,539 [24,533,917–24,606,085] | 12,714,215 [12,603,451–14,980,312] | 43,062,903 [40,293,435–44,968,065] |
| warm get uncleaned | 3,580 [3,376–3,586] | 543,135 [530,876–560,015] | 8,402 [8,151–9,423] |
| warm scan uncleaned | 6,127 [5,812–6,713] | 547,359 [534,664–551,894] | 11,728 [11,050–12,201] |
| clean elapsed | — | 384,809,409 [384,229,536–401,112,355] | — |
| clean write pause | — | 377,902,998 [377,108,348–394,474,739] | — |
| barrier after clean | — | 2,039 [1,271–2,250] | — |
| close after clean | — | 47,117 [39,735–149,876] | — |
| reopen post clean | — | 14,786,034 [13,887,271–15,373,661] | — |
| warm get post clean | — | 803,323 [801,275–932,562] | — |
| warm scan post clean | — | 898,336 [886,193–1,044,919] | — |
| close after verification | 150,426 [138,133–156,997] | — | 270,396 [184,989–284,878] |
| close post clean verification | — | 221,991 [158,367–237,967] | — |
| lifecycle sum | 263,874,907 [262,502,535–267,654,788] | 726,248,523 [719,862,792–740,868,787] | 269,710,295 [262,444,424–287,553,927] |

#### Experimental cleaning and post-clean reads

Natural Experimental root: **459,206 [459,206–459,206] B**; post-clean Experimental root: **475,486 [475,486–475,486] B**; natural RocksDB root: **412,662 [412,662–412,662] B**. These are different lifecycle states; cleaned Experimental bytes are not presented as equivalent to natural RocksDB.

Post-clean Experimental B/live byte: median 1.770581 (range 1.770581–1.770581). Natural Experimental B/live byte: median 1.709959.

| Clean stat | Median [min–max] |
|---|---:|
| Root bytes before | 459,206 [459,206–459,206] |
| Sampled peak bytes | 934,643 [934,643–934,643] |
| Root bytes after | 475,486 [475,486–475,486] |
| Deferred cleanup bytes | 0 [0–0] |
| Live records | 973 [973–973] |
| Live key+value bytes | 268,548 [268,548–268,548] |
| Copied payload bytes | 249,088 [249,088–249,088] |
| Copied transactions | 4 [4–4] |
| Index SSTables written | 1 [1–1] |
| Cleaner write pause ns | 377,902,998 [377,108,348–394,474,739] |
| Cleaner elapsed ns | 384,809,409 [384,229,536–401,112,355] |

Warm reads each sample: natural 8 get calls/8 entries + 1 scan/8 entries; post-clean Experimental same 8/8 + 1/8. Natural reads use `warm_get_uncleaned_ns`/`warm_scan_uncleaned_ns`; post-clean reads use separate timers. Experimental post-clean reopen median/range: 14,786,034 [13,887,271–15,373,661] ns. Cleaner `write_pause_ns` is nested in `elapsed_ns` and is not double-counted in lifecycle sum.

#### Experimental / RocksDB direction

| Timed phase | Experimental/RocksDB median time ratio | Difference (Exp − RocksDB) ns | Direction |
|---|---:|---:|---|
| append | 1.059949× | +6,675,624 | Experimental slower |
| mutations | 0.995696× | -211,275 | Experimental faster |
| reopen uncleaned | 0.295248× | -30,348,688 | Experimental faster |
| warm get uncleaned | 64.643537× | +534,733 | Experimental slower |
| warm scan uncleaned | 46.671129× | +535,631 | Experimental slower |
| lifecycle sum | 2.692699× | +456,538,228 | Experimental slower |
| Append throughput | 0.943442× rate | — | higher rate favors Experimental |
| Mutation throughput | 1.004322× rate | — | higher rate favors Experimental |

Footprint ratios: natural Experimental / natural RocksDB = 1.112790×; post-clean Experimental / natural RocksDB = 1.152241× (non-equivalent lifecycle stages).

Observed level counts and evidence:
- KV2 inline: sample 1: {} (ceredb_public_manifest_snapshot_while_closed); sample 2: {} (ceredb_public_manifest_snapshot_while_closed); sample 3: {} (ceredb_public_manifest_snapshot_while_closed)
- Experimental WAL: sample 1: {} (ceredb_public_manifest_snapshot_while_closed); sample 2: {} (ceredb_public_manifest_snapshot_while_closed); sample 3: {} (ceredb_public_manifest_snapshot_while_closed)
- RocksDB: sample 1: {"0": 1} (rocksdb_live_files_after_reopen); sample 2: {"0": 1} (rocksdb_live_files_after_reopen); sample 3: {"0": 1} (rocksdb_live_files_after_reopen)

### `v256_n1024_b256`

#### Timed writes and natural state

| Engine | Append ns [range] | Append ops/s | Append ops / calls / input B | Mutation ns [range] | Mutation ops/s | Mutation ops / calls / input B | Natural closed root B [range] | B/live byte | Actual final root B [range] |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| KV2 inline | 30,712,573 [30,401,131–31,689,231] | 33,341.394 | 1,024 / 4 / 282,624 | 14,075,441 [14,043,823–14,917,373] | 29,057.704 | 409 / 2 / 86,772 | 458,500 [458,500–458,500] | 1.707330 | 458,500 [458,500–458,500] |
| Experimental WAL | 30,330,642 [29,233,233–30,771,223] | 33,761.237 | 1,024 / 4 / 282,624 | 14,582,017 [13,832,207–14,682,073] | 28,048.246 | 409 / 2 / 86,772 | 458,577 [458,577–458,577] | 1.707617 | 475,487 [475,487–475,487] |
| RocksDB | 28,092,927 [26,288,944–33,192,650] | 36,450.456 | 1,024 / 4 / 282,624 | 14,104,420 [13,558,872–14,278,552] | 28,998.002 | 409 / 2 / 86,772 | 412,343 [412,343–412,343] | 1.535454 | 344,776 [344,776–344,776] |

#### Phase timings

| Phase | KV2 inline | Experimental WAL | RocksDB |
|---|---:|---:|---:|
| fresh open | 78,628,849 [76,694,134–84,615,530] | 129,583,588 [125,288,056–130,951,834] | 66,371,914 [65,436,021–67,244,326] |
| append | 30,712,573 [30,401,131–31,689,231] | 30,330,642 [29,233,233–30,771,223] | 28,092,927 [26,288,944–33,192,650] |
| mutations | 14,075,441 [14,043,823–14,917,373] | 14,582,017 [13,832,207–14,682,073] | 14,104,420 [13,558,872–14,278,552] |
| barrier drain | 2,438 [2,146–2,935] | 13,497 [10,779–14,838] | 4,022 [3,750–6,327,585] |
| close | 154,715 [145,075–215,705] | 12,808,313 [12,661,748–15,262,589] | 325,753 [240,643–408,531] |
| reopen uncleaned | 25,844,037 [25,153,136–28,743,638] | 12,962,606 [12,893,366–14,256,841] | 44,054,427 [41,395,612–44,381,449] |
| warm get uncleaned | 4,150 [3,757–4,545] | 853,763 [792,326–860,817] | 7,894 [7,079–8,467] |
| warm scan uncleaned | 6,365 [6,094–6,937] | 877,764 [877,282–901,069] | 11,223 [10,516–17,610] |
| clean elapsed | — | 419,077,643 [409,401,682–421,045,645] | — |
| clean write pause | — | 410,882,538 [402,557,206–414,389,915] | — |
| barrier after clean | — | 2,147 [1,406–16,004] | — |
| close after clean | — | 139,630 [43,903–277,786] | — |
| reopen post clean | — | 14,173,495 [13,946,073–14,564,221] | — |
| warm get post clean | — | 816,302 [801,184–816,576] | — |
| warm scan post clean | — | 966,169 [903,170–970,519] | — |
| close after verification | 146,112 [128,730–262,512] | — | 215,026 [206,225–254,369] |
| close post clean verification | — | 176,343 [151,919–180,521] | — |
| lifecycle sum | 152,535,374 [147,452,365–156,623,373] | 633,632,719 [633,120,885–637,021,205] | 153,746,494 [148,724,101–163,978,212] |

#### Experimental cleaning and post-clean reads

Natural Experimental root: **458,577 [458,577–458,577] B**; post-clean Experimental root: **475,487 [475,487–475,487] B**; natural RocksDB root: **412,343 [412,343–412,343] B**. These are different lifecycle states; cleaned Experimental bytes are not presented as equivalent to natural RocksDB.

Post-clean Experimental B/live byte: median 1.770585 (range 1.770585–1.770585). Natural Experimental B/live byte: median 1.707617.

| Clean stat | Median [min–max] |
|---|---:|
| Root bytes before | 458,577 [458,577–458,577] |
| Sampled peak bytes | 934,015 [934,015–934,015] |
| Root bytes after | 475,487 [475,487–475,487] |
| Deferred cleanup bytes | 0 [0–0] |
| Live records | 973 [973–973] |
| Live key+value bytes | 268,548 [268,548–268,548] |
| Copied payload bytes | 249,088 [249,088–249,088] |
| Copied transactions | 4 [4–4] |
| Index SSTables written | 1 [1–1] |
| Cleaner write pause ns | 410,882,538 [402,557,206–414,389,915] |
| Cleaner elapsed ns | 419,077,643 [409,401,682–421,045,645] |

Warm reads each sample: natural 8 get calls/8 entries + 1 scan/8 entries; post-clean Experimental same 8/8 + 1/8. Natural reads use `warm_get_uncleaned_ns`/`warm_scan_uncleaned_ns`; post-clean reads use separate timers. Experimental post-clean reopen median/range: 14,173,495 [13,946,073–14,564,221] ns. Cleaner `write_pause_ns` is nested in `elapsed_ns` and is not double-counted in lifecycle sum.

#### Experimental / RocksDB direction

| Timed phase | Experimental/RocksDB median time ratio | Difference (Exp − RocksDB) ns | Direction |
|---|---:|---:|---|
| append | 1.079654× | +2,237,715 | Experimental slower |
| mutations | 1.033862× | +477,597 | Experimental slower |
| reopen uncleaned | 0.294241× | -31,091,821 | Experimental faster |
| warm get uncleaned | 108.153408× | +845,869 | Experimental slower |
| warm scan uncleaned | 78.211173× | +866,541 | Experimental slower |
| lifecycle sum | 4.121282× | +479,886,225 | Experimental slower |
| Append throughput | 0.926223× rate | — | higher rate favors Experimental |
| Mutation throughput | 0.967248× rate | — | higher rate favors Experimental |

Footprint ratios: natural Experimental / natural RocksDB = 1.112125×; post-clean Experimental / natural RocksDB = 1.153135× (non-equivalent lifecycle stages).

Observed level counts and evidence:
- KV2 inline: sample 1: {} (ceredb_public_manifest_snapshot_while_closed); sample 2: {} (ceredb_public_manifest_snapshot_while_closed); sample 3: {} (ceredb_public_manifest_snapshot_while_closed)
- Experimental WAL: sample 1: {} (ceredb_public_manifest_snapshot_while_closed); sample 2: {} (ceredb_public_manifest_snapshot_while_closed); sample 3: {} (ceredb_public_manifest_snapshot_while_closed)
- RocksDB: sample 1: {"0": 1} (rocksdb_live_files_after_reopen); sample 2: {"0": 1} (rocksdb_live_files_after_reopen); sample 3: {"0": 1} (rocksdb_live_files_after_reopen)

### `v4096_n512_b1`

#### Timed writes and natural state

| Engine | Append ns [range] | Append ops/s | Append ops / calls / input B | Mutation ns [range] | Mutation ops/s | Mutation ops / calls / input B | Natural closed root B [range] | B/live byte | Actual final root B [range] |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| KV2 inline | 3,754,656,796 [3,750,556,376–3,829,198,519] | 136.364 | 512 / 512 / 2,107,392 | 1,497,998,527 [1,446,772,536–1,511,653,287] | 137.517 | 206 / 206 / 634,904 | 2,838,889 [2,838,889–2,838,889] | 1.419178 | 2,838,889 [2,838,889–2,838,889] |
| Experimental WAL | 3,633,117,338 [3,631,741,218–3,678,981,375] | 140.926 | 512 / 512 / 2,107,392 | 1,457,493,623 [1,446,796,621–1,507,894,780] | 141.339 | 206 / 206 / 634,904 | 2,813,563 [2,813,563–2,813,563] | 1.406517 | 2,104,213 [2,104,213–2,104,213] |
| RocksDB | 3,550,050,646 [3,545,899,906–3,557,328,702] | 144.223 | 512 / 512 / 2,107,392 | 1,403,472,098 [1,383,131,519–1,433,513,918] | 146.779 | 206 / 206 / 634,904 | 2,823,030 [2,823,030–2,823,030] | 1.411250 | 2,867,985 [2,867,985–2,867,985] |

#### Phase timings

| Phase | KV2 inline | Experimental WAL | RocksDB |
|---|---:|---:|---:|
| fresh open | 76,913,267 [67,920,568–79,913,175] | 129,259,913 [129,229,577–133,657,484] | 67,541,549 [62,365,067–68,014,444] |
| append | 3,754,656,796 [3,750,556,376–3,829,198,519] | 3,633,117,338 [3,631,741,218–3,678,981,375] | 3,550,050,646 [3,545,899,906–3,557,328,702] |
| mutations | 1,497,998,527 [1,446,772,536–1,511,653,287] | 1,457,493,623 [1,446,796,621–1,507,894,780] | 1,403,472,098 [1,383,131,519–1,433,513,918] |
| barrier drain | 2,274 [1,945–2,308] | 2,198 [1,774–4,064] | 3,715 [3,606–4,327] |
| close | 138,113 [97,639–196,380] | 13,682,151 [12,935,157–15,398,209] | 255,671 [226,563–381,978] |
| reopen uncleaned | 31,822,422 [29,468,539–35,084,750] | 16,029,194 [15,935,749–17,149,235] | 43,198,108 [41,541,849–44,739,287] |
| warm get uncleaned | 719,392 [717,457–736,573] | 469,335 [455,657–551,406] | 13,172 [12,823–14,491] |
| warm scan uncleaned | 240,899 [238,256–244,279] | 517,969 [468,845–548,793] | 36,672 [36,596–44,973] |
| clean elapsed | — | 391,196,145 [383,191,624–391,353,152] | — |
| clean write pause | — | 382,541,469 [375,398,079–383,427,947] | — |
| barrier after clean | — | 1,468 [1,403–1,643] | — |
| close after clean | — | 81,544 [79,282–179,479] | — |
| reopen post clean | — | 15,692,845 [14,675,369–16,467,057] | — |
| warm get post clean | — | 2,158,742 [2,050,955–2,370,330] | — |
| warm scan post clean | — | 2,414,098 [2,365,113–2,616,496] | — |
| close after verification | 58,586 [58,415–85,435] | — | 352,965 [339,392–391,587] |
| close post clean verification | — | 143,224 [136,764–144,989] | — |
| lifecycle sum | 5,373,849,047 [5,352,797,984–5,388,849,682] | 5,654,865,339 [5,651,318,790–5,763,459,258] | 5,062,754,162 [5,052,026,468–5,088,134,994] |

#### Experimental cleaning and post-clean reads

Natural Experimental root: **2,813,563 [2,813,563–2,813,563] B**; post-clean Experimental root: **2,104,213 [2,104,213–2,104,213] B**; natural RocksDB root: **2,823,030 [2,823,030–2,823,030] B**. These are different lifecycle states; cleaned Experimental bytes are not presented as equivalent to natural RocksDB.

Post-clean Experimental B/live byte: median 1.051909 (range 1.051909–1.051909). Natural Experimental B/live byte: median 1.406517.

| Clean stat | Median [min–max] |
|---|---:|
| Root bytes before | 2,813,563 [2,813,563–2,813,563] |
| Sampled peak bytes | 4,917,727 [4,917,727–4,917,727] |
| Root bytes after | 2,104,213 [2,104,213–2,104,213] |
| Deferred cleanup bytes | 0 [0–0] |
| Live records | 486 [486–486] |
| Live key+value bytes | 2,000,376 [2,000,376–2,000,376] |
| Copied payload bytes | 1,990,656 [1,990,656–1,990,656] |
| Copied transactions | 2 [2–2] |
| Index SSTables written | 1 [1–1] |
| Cleaner write pause ns | 382,541,469 [375,398,079–383,427,947] |
| Cleaner elapsed ns | 391,196,145 [383,191,624–391,353,152] |

Warm reads each sample: natural 8 get calls/8 entries + 1 scan/8 entries; post-clean Experimental same 8/8 + 1/8. Natural reads use `warm_get_uncleaned_ns`/`warm_scan_uncleaned_ns`; post-clean reads use separate timers. Experimental post-clean reopen median/range: 15,692,845 [14,675,369–16,467,057] ns. Cleaner `write_pause_ns` is nested in `elapsed_ns` and is not double-counted in lifecycle sum.

#### Experimental / RocksDB direction

| Timed phase | Experimental/RocksDB median time ratio | Difference (Exp − RocksDB) ns | Direction |
|---|---:|---:|---|
| append | 1.023399× | +83,066,692 | Experimental slower |
| mutations | 1.038491× | +54,021,525 | Experimental slower |
| reopen uncleaned | 0.371062× | -27,168,914 | Experimental faster |
| warm get uncleaned | 35.631263× | +456,163 | Experimental slower |
| warm scan uncleaned | 14.124373× | +481,297 | Experimental slower |
| lifecycle sum | 1.116954× | +592,111,177 | Experimental slower |
| Append throughput | 0.977136× rate | — | higher rate favors Experimental |
| Mutation throughput | 0.962935× rate | — | higher rate favors Experimental |

Footprint ratios: natural Experimental / natural RocksDB = 0.996647×; post-clean Experimental / natural RocksDB = 0.745374× (non-equivalent lifecycle stages).

Observed level counts and evidence:
- KV2 inline: sample 1: {"1": 2} (ceredb_public_manifest_snapshot_while_closed); sample 2: {"1": 2} (ceredb_public_manifest_snapshot_while_closed); sample 3: {"1": 2} (ceredb_public_manifest_snapshot_while_closed)
- Experimental WAL: sample 1: {} (ceredb_public_manifest_snapshot_while_closed); sample 2: {} (ceredb_public_manifest_snapshot_while_closed); sample 3: {} (ceredb_public_manifest_snapshot_while_closed)
- RocksDB: sample 1: {"0": 3} (rocksdb_live_files_after_reopen); sample 2: {"0": 3} (rocksdb_live_files_after_reopen); sample 3: {"0": 3} (rocksdb_live_files_after_reopen)

### `v4096_n512_b64`

#### Timed writes and natural state

| Engine | Append ns [range] | Append ops/s | Append ops / calls / input B | Mutation ns [range] | Mutation ops/s | Mutation ops / calls / input B | Natural closed root B [range] | B/live byte | Actual final root B [range] |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| KV2 inline | 225,847,574 [222,697,605–229,876,429] | 2,267.016 | 512 / 8 / 2,107,392 | 45,538,514 [40,131,654–49,538,486] | 4,523.643 | 206 / 4 / 634,904 | 2,831,511 [2,831,511–2,831,511] | 1.415489 | 2,831,511 [2,831,511–2,831,511] |
| Experimental WAL | 107,025,136 [86,468,123–109,399,976] | 4,783.923 | 512 / 8 / 2,107,392 | 44,421,285 [44,168,873–45,272,970] | 4,637.416 | 206 / 4 / 634,904 | 2,787,477 [2,787,477–2,787,477] | 1.393477 | 2,104,214 [2,104,214–2,104,214] |
| RocksDB | 82,521,164 [77,053,840–83,879,513] | 6,204.469 | 512 / 8 / 2,107,392 | 42,365,180 [42,163,450–42,878,408] | 4,862.484 | 206 / 4 / 634,904 | 2,820,164 [2,820,164–2,820,164] | 1.409817 | 2,868,065 [2,868,065–2,868,065] |

#### Phase timings

| Phase | KV2 inline | Experimental WAL | RocksDB |
|---|---:|---:|---:|
| fresh open | 75,409,895 [72,135,008–79,419,892] | 124,757,632 [122,295,549–131,191,855] | 65,371,369 [64,983,647–68,570,534] |
| append | 225,847,574 [222,697,605–229,876,429] | 107,025,136 [86,468,123–109,399,976] | 82,521,164 [77,053,840–83,879,513] |
| mutations | 45,538,514 [40,131,654–49,538,486] | 44,421,285 [44,168,873–45,272,970] | 42,365,180 [42,163,450–42,878,408] |
| barrier drain | 4,632 [1,928–9,796] | 2,280 [2,124–2,816] | 3,480 [3,474–46,677] |
| close | 37,592,157 [36,557,613–39,527,128] | 13,149,339 [13,045,776–14,011,309] | 222,059 [205,757–430,268] |
| reopen uncleaned | 33,259,592 [32,225,653–34,625,551] | 14,553,860 [13,909,392–15,678,883] | 43,184,780 [41,801,100–45,791,883] |
| warm get uncleaned | 886,880 [755,566–1,034,828] | 845,516 [839,243–859,809] | 11,870 [11,870–17,120] |
| warm scan uncleaned | 283,737 [238,229–284,214] | 834,843 [831,121–852,711] | 36,276 [35,548–65,724] |
| clean elapsed | — | 415,968,175 [404,314,686–450,179,589] | — |
| clean write pause | — | 408,511,895 [396,997,568–441,955,370] | — |
| barrier after clean | — | 1,663 [1,481–1,967] | — |
| close after clean | — | 47,713 [41,197–124,753] | — |
| reopen post clean | — | 16,127,601 [15,732,644–16,417,275] | — |
| warm get post clean | — | 2,171,806 [2,167,757–3,465,586] | — |
| warm scan post clean | — | 2,560,968 [2,497,505–3,672,875] | — |
| close after verification | 74,552 [65,278–77,567] | — | 368,648 [320,135–495,038] |
| close post clean verification | — | 150,484 [135,483–167,579] | — |
| lifecycle sum | 420,136,505 [411,055,640–426,907,813] | 746,067,028 [710,353,924–783,948,256] | 235,851,798 [229,381,408–237,605,606] |

#### Experimental cleaning and post-clean reads

Natural Experimental root: **2,787,477 [2,787,477–2,787,477] B**; post-clean Experimental root: **2,104,214 [2,104,214–2,104,214] B**; natural RocksDB root: **2,820,164 [2,820,164–2,820,164] B**. These are different lifecycle states; cleaned Experimental bytes are not presented as equivalent to natural RocksDB.

Post-clean Experimental B/live byte: median 1.051909 (range 1.051909–1.051909). Natural Experimental B/live byte: median 1.393477.

| Clean stat | Median [min–max] |
|---|---:|
| Root bytes before | 2,787,477 [2,787,477–2,787,477] |
| Sampled peak bytes | 4,891,642 [4,891,642–4,891,642] |
| Root bytes after | 2,104,214 [2,104,214–2,104,214] |
| Deferred cleanup bytes | 0 [0–0] |
| Live records | 486 [486–486] |
| Live key+value bytes | 2,000,376 [2,000,376–2,000,376] |
| Copied payload bytes | 1,990,656 [1,990,656–1,990,656] |
| Copied transactions | 2 [2–2] |
| Index SSTables written | 1 [1–1] |
| Cleaner write pause ns | 408,511,895 [396,997,568–441,955,370] |
| Cleaner elapsed ns | 415,968,175 [404,314,686–450,179,589] |

Warm reads each sample: natural 8 get calls/8 entries + 1 scan/8 entries; post-clean Experimental same 8/8 + 1/8. Natural reads use `warm_get_uncleaned_ns`/`warm_scan_uncleaned_ns`; post-clean reads use separate timers. Experimental post-clean reopen median/range: 16,127,601 [15,732,644–16,417,275] ns. Cleaner `write_pause_ns` is nested in `elapsed_ns` and is not double-counted in lifecycle sum.

#### Experimental / RocksDB direction

| Timed phase | Experimental/RocksDB median time ratio | Difference (Exp − RocksDB) ns | Direction |
|---|---:|---:|---|
| append | 1.296942× | +24,503,972 | Experimental slower |
| mutations | 1.048533× | +2,056,105 | Experimental slower |
| reopen uncleaned | 0.337014× | -28,630,920 | Experimental faster |
| warm get uncleaned | 71.231340× | +833,646 | Experimental slower |
| warm scan uncleaned | 23.013645× | +798,567 | Experimental slower |
| lifecycle sum | 3.163287× | +510,215,230 | Experimental slower |
| Append throughput | 0.771045× rate | — | higher rate favors Experimental |
| Mutation throughput | 0.953714× rate | — | higher rate favors Experimental |

Footprint ratios: natural Experimental / natural RocksDB = 0.988410×; post-clean Experimental / natural RocksDB = 0.746132× (non-equivalent lifecycle stages).

Observed level counts and evidence:
- KV2 inline: sample 1: {"1": 2} (ceredb_public_manifest_snapshot_while_closed); sample 2: {"1": 2} (ceredb_public_manifest_snapshot_while_closed); sample 3: {"1": 2} (ceredb_public_manifest_snapshot_while_closed)
- Experimental WAL: sample 1: {} (ceredb_public_manifest_snapshot_while_closed); sample 2: {} (ceredb_public_manifest_snapshot_while_closed); sample 3: {} (ceredb_public_manifest_snapshot_while_closed)
- RocksDB: sample 1: {"0": 3} (rocksdb_live_files_after_reopen); sample 2: {"0": 3} (rocksdb_live_files_after_reopen); sample 3: {"0": 3} (rocksdb_live_files_after_reopen)

### `v65536_n64_b1`

#### Timed writes and natural state

| Engine | Append ns [range] | Append ops/s | Append ops / calls / input B | Mutation ns [range] | Mutation ops/s | Mutation ops / calls / input B | Natural closed root B [range] | B/live byte | Actual final root B [range] |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| KV2 inline | 824,045,928 [802,163,990–830,221,091] | 77.666 | 64 / 64 / 4,195,584 | 294,419,191 [289,557,623–308,956,601] | 84.913 | 25 / 25 / 1,245,684 | 5,457,146 [5,457,146–5,457,146] | 1.364656 | 5,457,146 [5,457,146–5,457,146] |
| Experimental WAL | 521,886,205 [499,700,188–534,479,891] | 122.632 | 64 / 64 / 4,195,584 | 195,139,084 [189,183,652–197,864,001] | 128.114 | 25 / 25 / 1,245,684 | 5,450,372 [5,450,372–5,450,372] | 1.362962 | 4,012,678 [4,012,678–4,012,678] |
| RocksDB | 494,737,178 [484,890,307–497,835,646] | 129.362 | 64 / 64 / 4,195,584 | 210,218,966 [209,551,993–214,212,298] | 118.924 | 25 / 25 / 1,245,684 | 5,513,381 [5,513,381–5,513,381] | 1.378719 | 5,553,620 [5,553,620–5,553,620] |

#### Phase timings

| Phase | KV2 inline | Experimental WAL | RocksDB |
|---|---:|---:|---:|
| fresh open | 78,731,695 [75,236,841–79,896,977] | 127,326,662 [124,473,676–128,672,205] | 66,241,917 [66,065,392–69,371,004] |
| append | 824,045,928 [802,163,990–830,221,091] | 521,886,205 [499,700,188–534,479,891] | 494,737,178 [484,890,307–497,835,646] |
| mutations | 294,419,191 [289,557,623–308,956,601] | 195,139,084 [189,183,652–197,864,001] | 210,218,966 [209,551,993–214,212,298] |
| barrier drain | 2,273 [2,009–2,351] | 2,645 [2,009–2,684] | 5,766 [4,962–6,578,582] |
| close | 37,690 [29,975–132,015] | 14,396,368 [14,393,554–15,912,205] | 299,889 [262,335–374,608] |
| reopen uncleaned | 30,035,915 [29,182,163–30,902,758] | 17,700,871 [17,078,122–17,977,656] | 40,734,790 [37,696,836–44,261,678] |
| warm get uncleaned | 1,017,608 [1,011,747–1,093,395] | 601,433 [598,992–658,567] | 69,049 [67,478–73,373] |
| warm scan uncleaned | 762,970 [754,990–858,375] | 615,284 [610,414–659,638] | 116,157 [111,304–129,789] |
| clean elapsed | — | 310,867,668 [303,932,753–312,479,655] | — |
| clean write pause | — | 303,149,114 [296,267,591–303,783,867] | — |
| barrier after clean | — | 1,570 [1,290–1,713] | — |
| close after clean | — | 47,348 [42,702–160,184] | — |
| reopen post clean | — | 19,333,409 [17,517,923–20,430,437] | — |
| warm get post clean | — | 13,934,753 [13,059,986–15,725,403] | — |
| warm scan post clean | — | 13,347,413 [12,013,322–14,386,437] | — |
| close after verification | 37,902 [35,714–47,215] | — | 732,532 [697,843–841,543] |
| close post clean verification | — | 203,692 [111,861–224,342] | — |
| lifecycle sum | 1,232,617,475 [1,202,850,461–1,243,709,066] | 1,244,912,677 [1,197,770,163–1,245,077,027] | 815,618,690 [803,206,234–827,358,291] |

#### Experimental cleaning and post-clean reads

Natural Experimental root: **5,450,372 [5,450,372–5,450,372] B**; post-clean Experimental root: **4,012,678 [4,012,678–4,012,678] B**; natural RocksDB root: **5,513,381 [5,513,381–5,513,381] B**. These are different lifecycle states; cleaned Experimental bytes are not presented as equivalent to natural RocksDB.

Post-clean Experimental B/live byte: median 1.003441 (range 1.003441–1.003441). Natural Experimental B/live byte: median 1.362962.

| Clean stat | Median [min–max] |
|---|---:|
| Root bytes before | 5,450,372 [5,450,372–5,450,372] |
| Sampled peak bytes | 9,463,001 [9,463,001–9,463,001] |
| Root bytes after | 4,012,678 [4,012,678–4,012,678] |
| Deferred cleanup bytes | 0 [0–0] |
| Live records | 61 [61–61] |
| Live key+value bytes | 3,998,916 [3,998,916–3,998,916] |
| Copied payload bytes | 3,997,696 [3,997,696–3,997,696] |
| Copied transactions | 1 [1–1] |
| Index SSTables written | 1 [1–1] |
| Cleaner write pause ns | 303,149,114 [296,267,591–303,783,867] |
| Cleaner elapsed ns | 310,867,668 [303,932,753–312,479,655] |

Warm reads each sample: natural 8 get calls/8 entries + 1 scan/8 entries; post-clean Experimental same 8/8 + 1/8. Natural reads use `warm_get_uncleaned_ns`/`warm_scan_uncleaned_ns`; post-clean reads use separate timers. Experimental post-clean reopen median/range: 19,333,409 [17,517,923–20,430,437] ns. Cleaner `write_pause_ns` is nested in `elapsed_ns` and is not double-counted in lifecycle sum.

#### Experimental / RocksDB direction

| Timed phase | Experimental/RocksDB median time ratio | Difference (Exp − RocksDB) ns | Direction |
|---|---:|---:|---|
| append | 1.054876× | +27,149,027 | Experimental slower |
| mutations | 0.928266× | -15,079,882 | Experimental faster |
| reopen uncleaned | 0.434539× | -23,033,919 | Experimental faster |
| warm get uncleaned | 8.710235× | +532,384 | Experimental slower |
| warm scan uncleaned | 5.297003× | +499,127 | Experimental slower |
| lifecycle sum | 1.526342× | +429,293,987 | Experimental slower |
| Append throughput | 0.947979× rate | — | higher rate favors Experimental |
| Mutation throughput | 1.077278× rate | — | higher rate favors Experimental |

Footprint ratios: natural Experimental / natural RocksDB = 0.988572×; post-clean Experimental / natural RocksDB = 0.727807× (non-equivalent lifecycle stages).

Observed level counts and evidence:
- KV2 inline: sample 1: {"0": 1, "1": 4} (ceredb_public_manifest_snapshot_while_closed); sample 2: {"0": 1, "1": 4} (ceredb_public_manifest_snapshot_while_closed); sample 3: {"0": 1, "1": 4} (ceredb_public_manifest_snapshot_while_closed)
- Experimental WAL: sample 1: {} (ceredb_public_manifest_snapshot_while_closed); sample 2: {} (ceredb_public_manifest_snapshot_while_closed); sample 3: {} (ceredb_public_manifest_snapshot_while_closed)
- RocksDB: sample 1: {"0": 2, "6": 4} (rocksdb_live_files_after_reopen); sample 2: {"0": 2, "6": 4} (rocksdb_live_files_after_reopen); sample 3: {"0": 2, "6": 4} (rocksdb_live_files_after_reopen)

### `v65536_n64_b16`

#### Timed writes and natural state

| Engine | Append ns [range] | Append ops/s | Append ops / calls / input B | Mutation ns [range] | Mutation ops/s | Mutation ops / calls / input B | Natural closed root B [range] | B/live byte | Actual final root B [range] |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| KV2 inline | 332,291,130 [325,044,178–341,188,259] | 192.602 | 64 / 4 / 4,195,584 | 101,432,050 [98,363,005–103,037,039] | 246.470 | 25 / 2 / 1,245,684 | 10,713,608 [10,713,608–10,713,608] | 2.679128 | 10,713,608 [10,713,608–10,713,608] |
| Experimental WAL | 76,367,609 [73,116,601–77,356,126] | 838.052 | 64 / 4 / 4,195,584 | 46,556,216 [43,880,681–52,664,330] | 536.985 | 25 / 2 / 1,245,684 | 5,447,301 [5,447,301–5,447,301] | 1.362194 | 4,012,679 [4,012,679–4,012,679] |
| RocksDB | 72,160,286 [71,341,237–73,590,952] | 886.914 | 64 / 4 / 4,195,584 | 45,535,353 [39,839,765–48,176,663] | 549.024 | 25 / 2 / 1,245,684 | 5,510,086 [5,510,085–5,512,589] | 1.377895 | 5,556,236 [5,554,727–5,556,237] |

#### Phase timings

| Phase | KV2 inline | Experimental WAL | RocksDB |
|---|---:|---:|---:|
| fresh open | 78,349,653 [77,600,256–79,154,291] | 127,854,501 [124,474,440–128,762,206] | 65,477,708 [64,816,112–67,172,269] |
| append | 332,291,130 [325,044,178–341,188,259] | 76,367,609 [73,116,601–77,356,126] | 72,160,286 [71,341,237–73,590,952] |
| mutations | 101,432,050 [98,363,005–103,037,039] | 46,556,216 [43,880,681–52,664,330] | 45,535,353 [39,839,765–48,176,663] |
| barrier drain | 2,210 [2,121–2,838] | 2,200 [2,132–2,521] | 30,639 [30,596–31,814] |
| close | 86,287,636 [84,453,883–87,257,418] | 14,162,541 [13,471,290–16,570,955] | 516,317 [513,132–7,250,641] |
| reopen uncleaned | 28,733,552 [27,858,525–29,670,765] | 16,559,777 [14,638,736–16,821,907] | 49,672,415 [43,657,630–56,152,690] |
| warm get uncleaned | 1,061,670 [1,051,538–1,271,928] | 2,230,879 [2,125,389–2,440,288] | 69,915 [67,522–69,992] |
| warm scan uncleaned | 409,686 [398,809–554,868] | 2,111,249 [1,962,634–2,191,157] | 125,727 [120,460–126,776] |
| clean elapsed | — | 319,710,923 [314,144,979–322,006,136] | — |
| clean write pause | — | 311,844,559 [306,133,518–314,463,573] | — |
| barrier after clean | — | 1,407 [1,392–1,697] | — |
| close after clean | — | 46,324 [38,439–141,337] | — |
| reopen post clean | — | 17,938,638 [17,255,782–18,326,656] | — |
| warm get post clean | — | 12,823,731 [12,368,013–13,237,263] | — |
| warm scan post clean | — | 12,936,975 [12,409,277–13,012,330] | — |
| close after verification | 50,044 [37,404–126,456] | — | 3,838,614 [3,370,219–4,292,261] |
| close post clean verification | — | 130,712 [103,379–257,718] | — |
| lifecycle sum | 628,098,672 [622,011,999–635,580,541] | 647,076,581 [637,667,887–658,475,005] | 239,842,123 [230,696,685–247,508,897] |

#### Experimental cleaning and post-clean reads

Natural Experimental root: **5,447,301 [5,447,301–5,447,301] B**; post-clean Experimental root: **4,012,679 [4,012,679–4,012,679] B**; natural RocksDB root: **5,510,086 [5,510,085–5,512,589] B**. These are different lifecycle states; cleaned Experimental bytes are not presented as equivalent to natural RocksDB.

Post-clean Experimental B/live byte: median 1.003442 (range 1.003442–1.003442). Natural Experimental B/live byte: median 1.362194.

| Clean stat | Median [min–max] |
|---|---:|
| Root bytes before | 5,447,301 [5,447,301–5,447,301] |
| Sampled peak bytes | 9,459,931 [9,459,931–9,459,931] |
| Root bytes after | 4,012,679 [4,012,679–4,012,679] |
| Deferred cleanup bytes | 0 [0–0] |
| Live records | 61 [61–61] |
| Live key+value bytes | 3,998,916 [3,998,916–3,998,916] |
| Copied payload bytes | 3,997,696 [3,997,696–3,997,696] |
| Copied transactions | 1 [1–1] |
| Index SSTables written | 1 [1–1] |
| Cleaner write pause ns | 311,844,559 [306,133,518–314,463,573] |
| Cleaner elapsed ns | 319,710,923 [314,144,979–322,006,136] |

Warm reads each sample: natural 8 get calls/8 entries + 1 scan/8 entries; post-clean Experimental same 8/8 + 1/8. Natural reads use `warm_get_uncleaned_ns`/`warm_scan_uncleaned_ns`; post-clean reads use separate timers. Experimental post-clean reopen median/range: 17,938,638 [17,255,782–18,326,656] ns. Cleaner `write_pause_ns` is nested in `elapsed_ns` and is not double-counted in lifecycle sum.

#### Experimental / RocksDB direction

| Timed phase | Experimental/RocksDB median time ratio | Difference (Exp − RocksDB) ns | Direction |
|---|---:|---:|---|
| append | 1.058305× | +4,207,323 | Experimental slower |
| mutations | 1.022419× | +1,020,863 | Experimental slower |
| reopen uncleaned | 0.333380× | -33,112,638 | Experimental faster |
| warm get uncleaned | 31.908446× | +2,160,964 | Experimental slower |
| warm scan uncleaned | 16.792328× | +1,985,522 | Experimental slower |
| lifecycle sum | 2.697927× | +407,234,458 | Experimental slower |
| Append throughput | 0.944907× rate | — | higher rate favors Experimental |
| Mutation throughput | 0.978072× rate | — | higher rate favors Experimental |

Footprint ratios: natural Experimental / natural RocksDB = 0.988605×; post-clean Experimental / natural RocksDB = 0.728243× (non-equivalent lifecycle stages).

Observed level counts and evidence:
- KV2 inline: sample 1: {"1": 4} (ceredb_public_manifest_snapshot_while_closed); sample 2: {"1": 4} (ceredb_public_manifest_snapshot_while_closed); sample 3: {"1": 4} (ceredb_public_manifest_snapshot_while_closed)
- Experimental WAL: sample 1: {} (ceredb_public_manifest_snapshot_while_closed); sample 2: {} (ceredb_public_manifest_snapshot_while_closed); sample 3: {} (ceredb_public_manifest_snapshot_while_closed)
- RocksDB: sample 1: {"0": 5, "6": 1} (rocksdb_live_files_after_reopen); sample 2: {"0": 5, "6": 1} (rocksdb_live_files_after_reopen); sample 3: {"0": 5, "6": 1} (rocksdb_live_files_after_reopen)

## Independent audit and limits

- Pilot schema-2 functional run completed first; all three engines verified full scan (19 rows), 16 deterministic point gets, absent deleted/never-present probes, 8 natural gets/entries and one 8-entry scan. Experimental clean/reopen repeated exact verification and warm-read counts.
- Full report rows independently checked against eight requested case shapes, all 96 unique retained stores, 72 measured rows, 24 warmups, three samples per engine/case, one warmup per engine/case, and rotating engine order.
- Operation/batch/input-byte/live-row totals were recomputed from each execution row's case dimensions and mutation policy; all rows match. Full scans and read counts match expected live rows and deterministic bounded workloads.
- Every reported closed census category sum, file count, bytes/live ratio, and `lifecycle_sum_ns` was recomputed. Lifecycle uses each non-overlapping phase once; cleaner elapsed counted once; nested pause excluded from second addition.
- Independently walked every retained store root using filesystem metadata only, without opening any DB engine. All Experimental stores have exactly one remaining generation and no `other`-category files. Actual final root census is retained per store in `audit.json`; RocksDB post-reopen final census is kept distinct from its earlier natural closed census/live_files observation.
- Actual root census match summary (including warmups): `{"experimental_wal_values": {"execution_roots_audited_without_engine_open": 32, "roots_matching_latest_reported_closed_total": 32, "total_file_bytes_delta_max": 0, "total_file_bytes_delta_min": 0}, "kv2_inline": {"execution_roots_audited_without_engine_open": 32, "roots_matching_latest_reported_closed_total": 32, "total_file_bytes_delta_max": 0, "total_file_bytes_delta_min": 0}, "rocks_db": {"execution_roots_audited_without_engine_open": 32, "roots_matching_latest_reported_closed_total": 0, "total_file_bytes_delta_max": 47901, "total_file_bytes_delta_min": -94691}}`.
- Host: rustc 1.97.1 (8bab26f4f 2026-07-14); cargo 1.97.1 (c980f4866 2026-06-30); cc=cc (GCC) 16.2.1 20260819 (Red Hat 16.2.1-2); cxx=c++ (GCC) 16.2.1 20260819 (Red Hat 16.2.1-2); Linux fedora 7.1.13-200.fc44.x86_64 #1 SMP PREEMPT_DYNAMIC Wed Sep  2 13:58:38 UTC 2026 x86_64 GNU/Linux; CPU(s):                                  8; Model name:                              11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz; MemTotal:       16075928 kB; MemAvailable:    9854848 kB. No CPU affinity or power-loss test. Three low-record samples per case; warm cache and Btrfs CoW/compression affect results. This is not sustained-capacity evidence.
- CereDB uses public async APIs; RocksDB uses synchronous calls on benchmark thread with sync WAL. CereDB's write API clones owned vectors; RocksDB takes borrowed slices and copies internally. Those API costs remain inside write timers; APIs are not identical at allocation/scheduling boundaries.
- Append/mutation rates count only actual timed append/mutation operations, never unclocked setup or probes. They are derived from median phase time, not per-operation latency distributions.
- Experimental clean-and-repack is explicit; its pause, elapsed time, sampled peak, before/after/deferred bytes, payload bytes, transaction count, and index SSTable count are reported. Sampled peak is not continuous. Cleaned Experimental state is not RocksDB's natural closed state.
- Natural KV2 and RocksDB were not force-flushed or manually compacted. RocksDB drop cancels/unschedules pending jobs and waits outstanding jobs; it does not prove all maintenance completed. Level metadata is observed at the report's stated point, not equalized.
- The pre-reopen natural census and RocksDB live_files-after-reopen level counts are distinct moments. The final on-disk census is independently captured after process close; new metadata can make it differ from the earlier closed census.
- Fresh-open timers include startup/recovery work; no profiler attributes causal costs. FTS is not included and no FTS performance result is claimed. The earlier patterned 256 B/1k run with its 251-value pattern differs from this fixed-seed high-entropy workload and current source; qualitative context only, never a numerical before/after baseline. No etcd comparison.
- These results establish this matched local run only: no power-loss safety, external service parity, general speedup, or capacity claim.
