# JDblance -- C++20, Linux, no dependencies beyond a compiler and make.
#
#   make            build everything into build/
#   make test       unit + end-to-end tests, then the process-level smoke test
#   make asan       the same tests under AddressSanitizer + UBSan
#   make demo       chaos demo: break backends, watch the client not notice

CXX      ?= g++
BUILD    ?= build
SAN      ?=
OPT      ?= -O2
CXXFLAGS += -std=c++20 $(OPT) -g -pipe -Wall -Wextra -Wpedantic -Isrc -MMD -MP
LDFLAGS  += -pthread
ifeq ($(SAN),1)
CXXFLAGS += -fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=undefined
LDFLAGS  += -fsanitize=address,undefined
endif

COMPOSE ?= docker compose
LB      ?= http://localhost:8000
ADMIN   ?= http://localhost:8001
RATE    ?= 2000
DUR     ?= 20

LIB_SRCS  := $(wildcard src/*.cpp)
LIB_OBJS  := $(LIB_SRCS:%.cpp=$(BUILD)/obj/%.o)
TEST_SRCS := $(wildcard tests/*.cpp)
TEST_OBJS := $(TEST_SRCS:%.cpp=$(BUILD)/obj/%.o)
TOOLS     := jdblance jdblance-origin jdblance-loadgen jdblance-demo jdblance-alertbot
BINS      := $(TOOLS:%=$(BUILD)/%) $(BUILD)/jdblance-tests

.PHONY: all test unit smoke asan check demo run bench bench-nginx compare up down logs \
        chaos heal stats reload drain clean help

all: $(BINS)

help:
	@echo "make              build everything into $(BUILD)/"
	@echo "make test         unit + e2e tests, then the process-level smoke test"
	@echo "make asan         the same under AddressSanitizer + UBSan"
	@echo "make demo         chaos demo: break backends, watch the client not notice"
	@echo "make run          the balancer locally against 4 local origins"
	@echo "make up           the full Docker stack"
	@echo "make bench        load test the balancer   (RATE=$(RATE) DUR=$(DUR))"
	@echo "make compare      same load against JDblance and nginx, side by side"
	@echo "make chaos        inject failures into backend1/backend2 in the running stack"
	@echo "make stats        the control plane's view of the fleet"
	@echo "make reload       hot-reload lb.json with no dropped connections"

$(BUILD)/obj/%.o: %.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD)/jdblance: $(BUILD)/obj/cmd/jdblance.o $(LIB_OBJS)
	$(CXX) $^ -o $@ $(LDFLAGS)

$(BUILD)/jdblance-origin: $(BUILD)/obj/cmd/origin.o $(LIB_OBJS)
	$(CXX) $^ -o $@ $(LDFLAGS)

$(BUILD)/jdblance-loadgen: $(BUILD)/obj/cmd/loadgen.o $(LIB_OBJS)
	$(CXX) $^ -o $@ $(LDFLAGS)

$(BUILD)/jdblance-demo: $(BUILD)/obj/cmd/demo.o $(LIB_OBJS)
	$(CXX) $^ -o $@ $(LDFLAGS)

$(BUILD)/jdblance-alertbot: $(BUILD)/obj/cmd/alertbot.o $(LIB_OBJS)
	$(CXX) $^ -o $@ $(LDFLAGS)

$(BUILD)/jdblance-tests: $(TEST_OBJS) $(LIB_OBJS)
	$(CXX) $^ -o $@ $(LDFLAGS)

-include $(shell find $(BUILD)/obj -name '*.d' 2>/dev/null)

test: unit smoke

unit: $(BUILD)/jdblance-tests
	$(BUILD)/jdblance-tests

smoke: $(TOOLS:%=$(BUILD)/%)
	BUILD=$(BUILD) bash tests/smoke.sh

asan:
	$(MAKE) BUILD=build-asan SAN=1 OPT=-O1 test

check: $(BUILD)/jdblance
	$(BUILD)/jdblance --check -c lb.json

demo: $(BUILD)/jdblance-demo
	$(BUILD)/jdblance-demo

# Four origins plus the balancer, all local, no Docker.  Ctrl-C stops everything.
run: $(BUILD)/jdblance $(BUILD)/jdblance-origin
	@echo "origins on 9001-9004, balancer on 8000, control plane on 8001"
	@trap 'kill 0' INT TERM EXIT; \
	 $(BUILD)/jdblance-origin --port 9001 --name local1 --latency-ms 3 & \
	 $(BUILD)/jdblance-origin --port 9002 --name local2 --latency-ms 3 & \
	 $(BUILD)/jdblance-origin --port 9003 --name local3 --latency-ms 3 & \
	 $(BUILD)/jdblance-origin --port 9004 --name local4 --latency-ms 40 & \
	 sleep 0.3; \
	 $(BUILD)/jdblance -c lb.json --listen 127.0.0.1:8000 \
	   --endpoint local1=127.0.0.1:9001 --endpoint local2=127.0.0.1:9002 \
	   --endpoint local3=127.0.0.1:9003 --endpoint local4=127.0.0.1:9004

up:
	$(COMPOSE) up --build -d
	@echo "balancer   $(LB)"
	@echo "dashboard  $(ADMIN)"
	@echo "grafana    http://localhost:3001  (dashboard is pre-provisioned)"
	@echo "prometheus http://localhost:9091"
	@echo "nginx      http://localhost:8080  (control group)"

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f jdblance

bench: $(BUILD)/jdblance-loadgen
	$(BUILD)/jdblance-loadgen $(LB)/ --rate $(RATE) -d $(DUR)

bench-nginx: $(BUILD)/jdblance-loadgen
	$(BUILD)/jdblance-loadgen http://localhost:8080/ --rate $(RATE) -d $(DUR)

compare: $(BUILD)/jdblance-loadgen
	@echo "=== JDblance (adaptive) ==="
	@$(BUILD)/jdblance-loadgen $(LB)/ --rate $(RATE) -d $(DUR)
	@echo "=== nginx least_conn (control group) ==="
	@$(BUILD)/jdblance-loadgen http://localhost:8080/ --rate $(RATE) -d $(DUR)

chaos:
	curl -s -X POST localhost:5001/chaos -H 'Content-Type: application/json' -d '{"fail_rate":1.0}' && echo
	curl -s -X POST localhost:5002/chaos -H 'Content-Type: application/json' -d '{"latency_ms":400}' && echo

heal:
	@for port in 5001 5002 5003 5004; do \
	  curl -s -X POST localhost:$$port/chaos -H 'Content-Type: application/json' \
	    -d '{"reset":true}' && echo; \
	done

stats:
	@curl -s $(ADMIN)/stats && echo

drain:
	curl -s -X POST $(ADMIN)/endpoints/backend1/drain && echo

reload:
	curl -s -X POST $(ADMIN)/reload && echo

clean:
	rm -rf build build-asan
