# The 90-second terminal demo of the factory example, recorded with VHS
# (https://github.com/charmbracelet/vhs). Start the demo first (docker compose up -d, wait a
# few minutes so there is data), then from the repository root:  vhs examples/factory/demo.tape

Output docs/assets/factory-demo.gif
Output docs/assets/factory-demo.mp4

Set Shell "bash"
Set FontSize 17
Set Width 1400
Set Height 800
Set Padding 24
Set Theme "Dracula"
Set TypingSpeed 22ms

Hide
Type "cd examples/factory && export PGPASSWORD=demo-token-not-secret LC_ALL=C PS1='$ ' PSQLRC=/dev/null && alias q='psql -h 127.0.0.1 -p 55432 -U ankaa -d ankaa -P footer=off -c'" Enter
Type "clear" Enter
Show

Type "# 20 machines publish to an MQTT Unified Namespace; ANKAA turns topic segments into columns" Enter
Sleep 600ms
Type "grep -E '^(subject|map)' ankaad.toml" Enter
Sleep 3.5s

Type "# every reading is queryable milliseconds after it arrives" Enter
Type "q 'SELECT count(*) AS readings FROM telemetry'; sleep 2; q 'SELECT count(*) AS readings FROM telemetry'" Enter
Sleep 4.5s

Type "# a reading without a value is NULL, not zero: aggregates skip it" Enter
Type `q "SELECT machine, count(*) AS readings, count(kw) AS with_kw, round(avg(kw) / 1e8, 2) AS avg_kw FROM telemetry GROUP BY 1 ORDER BY readings - with_kw DESC LIMIT 4"` Enter
Sleep 5s

Type "# live 1-minute bars per machine, maintained as readings arrive (a continuous query)" Enter
Type `q "SELECT machine, open / 1e8 AS open_kw, high / 1e8 AS high_kw, close / 1e8 AS close_kw, count FROM cq_power_1m_live WHERE ts = (SELECT max(ts) FROM cq_power_1m_live) ORDER BY high_kw DESC LIMIT 4"` Enter
Sleep 5s

Type "# line 4's gateway goes offline and sends late: readings are placed in time, nothing is lost" Enter
Type "curl -s -H 'Authorization: Bearer demo-token-not-secret' 127.0.0.1:9100/metrics | grep -E '^ankaa_(late_rows_total|user_rows_total|user_rejected_total) '" Enter
Sleep 4s

Type "# the same SQL over Arrow Flight, Python and Grafana: github.com/ualsweb/ankaa" Enter
Sleep 3s
