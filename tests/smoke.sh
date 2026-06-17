#!/usr/bin/env bash
# Process-level smoke test: what the in-process suite cannot reach -- the CLI,
# signals, SIGHUP reload, forked workers and respawn, graceful drain, and the
# tools end to end.  Needs bash and curl.   Usage: BUILD=build tests/smoke.sh
set -uo pipefail

B=$(cd "${BUILD:-build}" && pwd)
REPO=$(pwd)
WORK=$(mktemp -d "${TMPDIR:-/tmp}/jdblance-smoke.XXXXXX")
PIDS=()
FAILED=0

cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null; done
  wait 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

ok() { echo "  ok    $1"; }
bad() {
  echo "  FAIL  $1"
  FAILED=1
}
# check "description" command...   (passes if the command succeeds)
check() {
  local what=$1
  shift
  if "$@"; then ok "$what"; else bad "$what"; fi
}
# Polls until the command succeeds, up to ~5 s.
eventually() {
  for _ in $(seq 100); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.05
  done
  return 1
}
now_ms() { echo $(($(date +%s%N) / 1000000)); }
stats_has() { curl -s "http://127.0.0.1:$AP/stats" | grep -q "$1"; }
all_200() {
  local codes
  codes=$(for _ in $(seq "$1"); do curl -s -o /dev/null -w '%{http_code}\n' "http://127.0.0.1:$LP/"; done | sort -u)
  [ "$codes" = "200" ]
}
alertbot() { (cd "$WORK" && "$B/jdblance-alertbot" "$@"); }   # never pick up a real ./.env

base=$((20000 + RANDOM % 20000))
O1=$base O2=$((base + 1)) O3=$((base + 2)) LP=$((base + 3)) AP=$((base + 10))

for i in 1 2 3; do
  "$B/jdblance-origin" --host 127.0.0.1 --port $((base + i - 1)) --name "o$i" --latency-ms 1 >"$WORK/o$i.log" 2>&1 &
  PIDS+=($!)
done
for i in 1 2 3; do eventually curl -sf "http://127.0.0.1:$((base + i - 1))/health" || bad "origin o$i started"; done

write_config() {   # policy endpoints-json
  cat >"$WORK/lb.json" <<EOF
{
  // smoke-test config
  "listen": {"host": "127.0.0.1", "port": $LP, "workers": \${SMOKE_WORKERS:-1}},
  "admin": {"host": "127.0.0.1", "port": $AP},
  "policy": "$1",
  "endpoints": $2,
  "health": {"interval": 0.5, "timeout": 0.5},
  "log": {"access": false},
  "timeouts": {"drain": 3}
}
EOF
}
two='[{"name": "o1", "address": "127.0.0.1:'$O1'"}, {"name": "o2", "address": "127.0.0.1:'$O2'"}]'
three='[{"name": "o1", "address": "127.0.0.1:'$O1'"}, {"name": "o2", "address": "127.0.0.1:'$O2'"},
        {"name": "o3", "address": "127.0.0.1:'$O3'"}]'

echo "cli"
write_config p2c_ewma "$two"
check "--check accepts the shipped config" bash -c "'$B/jdblance' --check -c '$REPO/lb.json' | grep -q 'config OK'"
check "--check accepts the test config" bash -c "'$B/jdblance' --check -c '$WORK/lb.json' >/dev/null"
echo '{"endpoints": [{"name": "a", "address": "h:0"}]}' >"$WORK/bad.json"
"$B/jdblance" --check -c "$WORK/bad.json" >/dev/null 2>&1
check "an invalid config exits 2" [ $? -eq 2 ]
"$B/jdblance" --bogus >/dev/null 2>&1
check "an unknown argument exits 2" [ $? -eq 2 ]
check "--version" bash -c "'$B/jdblance' --version | grep -q 'jdblance 1'"

echo "single worker: serve, reload, drain"
"$B/jdblance" -c "$WORK/lb.json" >"$WORK/jdblance.log" 2>&1 &
HP=$!
PIDS+=($HP)
check "becomes ready" eventually curl -sf "http://127.0.0.1:$AP/readyz"
check "20 requests, all 200" all_200 20

write_config round_robin "$three"
kill -HUP "$HP"
check "SIGHUP applies the new policy" eventually stats_has '"policy":"round_robin"'
check "...and the new endpoint set" stats_has '"total":3'
seen=$(for _ in $(seq 9); do curl -s -o /dev/null -w '%header{x-upstream}\n' "http://127.0.0.1:$LP/"; done | sort -u | tr '\n' ' ')
check "the added endpoint takes traffic ($seen)" [ "$seen" = "o1 o2 o3 " ]

echo '{ broken' >"$WORK/lb.json"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$AP/reload")
check "a broken config is refused with 400" [ "$code" = 400 ]
check "...and the old config keeps serving" all_200 3

# An idle keep-alive connection must not hold the drain open until client_idle.
(exec 3<>"/dev/tcp/127.0.0.1/$LP" && printf 'GET / HTTP/1.1\r\nHost: t\r\n\r\n' >&3 && sleep 10) &
IDLE=$!
sleep 0.3
start=$(now_ms)
kill -TERM "$HP"
wait "$HP"
status=$?
took=$(($(now_ms) - start))
kill "$IDLE" 2>/dev/null
check "SIGTERM drains and exits 0 promptly (exit $status, ${took} ms)" test $status -eq 0 -a $took -lt 2000
check "...and logs it" grep -q 'worker 0 stopped' "$WORK/jdblance.log"

echo "command-line overrides survive a reload"
write_config p2c_ewma "$three"
"$B/jdblance" -c "$WORK/lb.json" --endpoint "solo=127.0.0.1:$O1" --listen "127.0.0.1:$LP" >"$WORK/jdblance2.log" 2>&1 &
HP=$!
PIDS+=($HP)
eventually curl -sf "http://127.0.0.1:$AP/readyz"
kill -HUP "$HP"
check "reload succeeded with the CLI settings" eventually grep -q 'SIGHUP reload: policy=p2c_ewma endpoints=1' "$WORK/jdblance2.log"
check "...still one endpoint" stats_has '"total":1'
kill -TERM "$HP"
wait "$HP"

echo "forked workers"
SMOKE_WORKERS=2 "$B/jdblance" -c "$WORK/lb.json" >"$WORK/jdblance3.log" 2>&1 &
SUP=$!
PIDS+=($SUP)
check "worker 0 control plane" eventually curl -sf "http://127.0.0.1:$AP/healthz"
check "worker 1 control plane" eventually curl -sf "http://127.0.0.1:$((AP + 1))/healthz"
check "the shared SO_REUSEPORT port serves" all_200 20
victim=$(pgrep -P "$SUP" | head -1)
kill -KILL "$victim"
two_children() { [ "$(pgrep -P "$SUP" | wc -l)" -eq 2 ] && ! pgrep -P "$SUP" | grep -qx "$victim"; }
check "a killed worker is respawned" eventually two_children
check "...and serves again" eventually curl -sf "http://127.0.0.1:$AP/healthz"
check "...and the respawn was logged" grep -q 'respawning' "$WORK/jdblance3.log"
workers=$(pgrep -P "$SUP" | tr '\n' ' ')
kill -TERM "$SUP"
wait "$SUP"
status=$?
check "the supervisor exits 0 once its workers drain (exit $status)" [ $status -eq 0 ]
none_alive() {
  for pid in $workers; do kill -0 "$pid" 2>/dev/null && return 1; done
  return 0
}
check "no worker is left behind ($workers)" none_alive

echo "tools"
"$B/jdblance" -c "$WORK/lb.json" >"$WORK/jdblance4.log" 2>&1 &
HP=$!
PIDS+=($HP)
eventually curl -sf "http://127.0.0.1:$AP/readyz"
check "loadgen: 500 rps open loop with zero errors" \
  bash -c "'$B/jdblance-loadgen' http://127.0.0.1:$LP/ --rate 500 -d 2 --max-error-rate 0 >'$WORK/loadgen.txt'"
check "loadgen report" grep -q 'corrected for coordinated omission' "$WORK/loadgen.txt"
check "loadgen --json, closed loop" bash -c "'$B/jdblance-loadgen' http://127.0.0.1:$LP/ -c 4 -d 0.5 --json | grep -q '\"throughput_rps\"'"

curl -s -X POST "http://127.0.0.1:$O2/chaos?healthy=0" >/dev/null
check "health checks eject the endpoint" eventually stats_has '"name":"o2","address":"127.0.0.1:'$O2'","weight":1,"healthy":false'
alertbot --admin "http://127.0.0.1:$AP" --once >"$WORK/alert.txt" 2>&1
check "alertbot reports it" grep -q 'DOWN: o2' "$WORK/alert.txt"
TWILIO_ACCOUNT_SID=AC1 TWILIO_AUTH_TOKEN=secret TWILIO_WHATSAPP_NUMBER=a YOUR_WHATSAPP_NUMBER=b \
  TWILIO_API_BASE="http://127.0.0.1:$O3" alertbot --admin "http://127.0.0.1:$AP" --once >"$WORK/alert2.txt" 2>&1
check "alertbot delivers through curl" bash -c "grep -q 'twilio enabled' '$WORK/alert2.txt' && ! grep -q 'delivery failed' '$WORK/alert2.txt'"
alertbot --admin "http://127.0.0.1:1" --once >"$WORK/alert3.txt" 2>&1
check "alertbot notices an unreachable control plane" grep -q 'CRITICAL' "$WORK/alert3.txt"
kill -TERM "$HP"
wait "$HP"

if [ $FAILED -ne 0 ]; then
  echo "smoke: FAILED"
  exit 1
fi
echo "smoke: all passed"
