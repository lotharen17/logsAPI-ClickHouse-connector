**Scrypt to:**

1. Download Yandex.Metrika Logs API data (source and fields are specified in source_fields.json file; Metrika tag/counter and token are specified in token_counter.json file).
2. Upload this TSV data to your local machine clickhouse (in case ssh_path variable is commented in script code) or to remote server through ssh (in case you uncomment ssh_path variable in source code).
   
**Prerequisites:**
1. You should have Metrika counter and access to read its data: https://yandex.com/support/metrica/general/creating-counter.html). 
2. Create OAuth Token for Yandex with neccessary rights for Metrika: https://oauth.yandex.com/ and https://yandex.ru/dev/metrika/en/intro/authorization .
3. Create DB and table in ClickHouse to load data there.


**Config files:**
1. ch_credentials.json - login, password, host, port of clickhouse server to connect.
2. source_fields.json - fields to load from Logs API and source (visits or hits).
3. ssh_credentials.json - in case you need ssh connection to remote server (don't forget to uncomment ssh_path in extraction_script.py code) - contains login, pass, host (remote host), port  (22 by default) and remote port address to bind (default - 8123). Local port to bind is specified by
port address in ch_credentials.json file. So be aware).
4. token_counter - contains your yandex OAuth and tag/counter number. 
