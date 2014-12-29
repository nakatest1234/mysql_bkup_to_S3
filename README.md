mysql_bkup_to_S3
================

# 使い方
AWS_CONFIG_FILE=~/.aws/config /bin/sh ~/mysql_bkup_to_S3.sh ~/mysql.bkup.conf

# 生成されるファイル

* {prefix.}SERVER.SCHEME.YMDHIS.create.sql{.gz}
* {prefix.}SERVER.SCHEME.YMDHIS.data.sql{.gz}
* {prefix.}SERVER.SCHEME.YMDHIS.log.sql{.gz}

# mysql.bkup.conf
設定ファイル

| 変数名                  | 例           | 用途 |
| ----------------------- | ------------ | ---- |
| SERVER                  | 'mysql1234'  | mysqlサーバ |
| SCHEME                  | 'naka1234'   | db名 |
| ID                      | 'naka1234'   | id   |
| PW                      | 'naka1234'   | pw   |
| BKUPFILE_PREF           | 'bkup.mysql' | ファイル名のプリフィックス |
| TMPDIR                  | '/tmp/mysql' | dumpデータを置く場所。このディレクトリ配下に${SERVER}/${SCHEME}が作られる |
| FLG_GZIP                | 'y'          | gzip圧縮(y,n) |
| FLG_RESET_AUTOINCREMENT | 'n'          | auto incrementをリセットする(y,n) |
| FLG_BKUP_DELETE         | 'y'          | ローカルに出力したdumpデータを削除するか |
| BKUP_DAYS               | '3'          | 指定した日を経過したdumpデータが消える   |
| OPT_BASE                | "--quick --add-drop-table --add-locks --extended-insert --order-by-primary --single-transaction" | お好みで |
| TABLE_IGNORE            | ()           | リスト形式でデータダンプを行わないテーブルを指定 |
| TABLE_LOG               | ()           | ここに指定したテーブルはlog.sqlに別途データダンプされます |
| S3_DIR                  | "s3-bucket/mysql/${SERVER}/${SCHEME}" | S3のバケットを指定 |

