.PHONY: all build all-platforms win win.arm64 linux linux.arm64 darwin darwin.amd64 run check clean
.DEFAULT_GOAL := build

DST_DIR=dist
BIN_FILE=kmq
MAIN_PROG=./cmd    # 必须要带有./前缀, 才能强制忽略标准库和依赖查找机制, 直接从当前目录开始查找, 否则会去标准库或依赖项中寻找, 例如: 直接去 $GOROOT/src/cmd/main 寻找，找不到了便报 not in std
Version=0.6.15
Author=Liu Kun

# 链接期注入参数(-w -s 去符号表, -X 注入版本信息)
param=-w -s -X main.BuildVersion=${Version} -X \"main.BuildTime=${BuildDate}\" -X \"main.BuildPerson=${Author}\" -X \"main.BuildName=${BIN_FILE}\"

# ---- OS 检测(解析期确定): uname_S=平台名, SHELL ----
ifeq ($(OS),Windows_NT)
    SHELL=cmd.exe
    uname_S := Windows
    tmp_lint=$(shell cmd /C "where /Q golangci-lint && echo YES||echo NO")
    BuildDate=$(shell powershell -Command "Get-Date -Format 'yyyy-MM-dd HH:mm:ss'")
else
    SHELL=/bin/sh
    uname_S := $(shell uname -s)
    tmp_lint=$(shell which golangci-lint > /dev/null 2>&1 && echo YES || echo NO)
    BuildDate=$(shell date +"%F %T")
endif

# ---- 交叉编译命令模板: 仅按 shell 语法分 2 支 (cmd / sh) ----
# GO_BUILD 参数: $(1)=CGO_ENABLED $(2)=GOOS $(3)=GOARCH $(4)=产物路径 $(5)=主程序包路径 (均显式传入)
ifeq ($(uname_S),Windows)
    GO_BUILD = cmd /C 'set CGO_ENABLED=$(1)&&set GOOS=$(2)&&set GOARCH=$(3)&&go build -v -ldflags '${param}' -o $(4) $(5)'
    BUILD_MSG = powershell -Command "Write-Host \"$(1)\" -ForegroundColor green"
else
    GO_BUILD = export CGO_ENABLED=$(1); export GOOS=$(2); export GOARCH=$(3); go build -v -ldflags "${param}" -o $(4) $(5)
    BUILD_MSG = printf '\033[0;32m %s\033[0m\n' '$(1)'
endif

# golangci-lint 未安装时, check 执行到 ${LINT_GUARD} 那一行才触发 $(error)
ifeq (${tmp_lint}, NO)
    LINT_GUARD = $(error golangci-lint not found!  try run 'go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest')
endif

# ---- 本机平台判定: build 的默认目标 / run 的产物路径 / clean 的清理命令 ----
HOST_ARCH := $(shell go env GOARCH)
RM_DIST = rm --force ${DST_DIR}/*
ifeq ($(uname_S),Windows)
    BUILD_TARGET := $(if $(filter arm64,${HOST_ARCH}),win.arm64,win)
    RUN_BIN = ${DST_DIR}/${BIN_FILE}.exe
    RM_DIST = cmd /C 'del /F /Q ${DST_DIR}\\*'
else ifeq ($(uname_S),Darwin)
    BUILD_TARGET := $(if $(filter arm64,${HOST_ARCH}),darwin,darwin.amd64)
    RUN_BIN = ${DST_DIR}/${BIN_FILE}.darwin$(if $(filter amd64,${HOST_ARCH}),.amd64)
else
    BUILD_TARGET := $(if $(filter arm64,${HOST_ARCH}),linux.arm64,linux)
    RUN_BIN = ${DST_DIR}/${BIN_FILE}$(if $(filter arm64,${HOST_ARCH}),.arm64)
endif

all: all-platforms

build: $(BUILD_TARGET)   # 只编译当前宿主 OS 对应的目标

all-platforms: win win.arm64 linux linux.arm64 darwin darwin.amd64  # 编译所有平台（手动触发）
	@echo All platforms built

win:   # 输出windows amd64平台的编译结果
	@$(call GO_BUILD,1,windows,amd64,${DST_DIR}/${BIN_FILE}.exe,${MAIN_PROG})
	@$(call BUILD_MSG,Build windows 64bit program - ${DST_DIR}/${BIN_FILE}.exe)

win.arm64:  # 输出windows arm64平台的编译结果
	@$(call GO_BUILD,0,windows,arm64,${DST_DIR}/${BIN_FILE}.arm64.exe,${MAIN_PROG})
	@$(call BUILD_MSG,Build windows arm64bit program - ${DST_DIR}/${BIN_FILE}.arm64.exe)

linux:  # 输出linux amd64平台的编译结果
	@$(call GO_BUILD,0,linux,amd64,${DST_DIR}/${BIN_FILE},${MAIN_PROG})
	@$(call BUILD_MSG,Build linux 64bit program - ${DST_DIR}/${BIN_FILE})

linux.arm64:  # 输出linux arm64平台的编译结果
	@$(call GO_BUILD,0,linux,arm64,${DST_DIR}/${BIN_FILE}.arm64,${MAIN_PROG})
	@$(call BUILD_MSG,Build linux arm64bit program - ${DST_DIR}/${BIN_FILE}.arm64)

darwin:  # 输出darwin arm64平台的编译结果
	@$(call GO_BUILD,0,darwin,arm64,${DST_DIR}/${BIN_FILE}.darwin,${MAIN_PROG})
	@$(call BUILD_MSG,Build MacOS arm64bit program - ${DST_DIR}/${BIN_FILE}.darwin)

darwin.amd64:  # 输出darwin amd64平台的编译结果
	@$(call GO_BUILD,0,darwin,amd64,${DST_DIR}/${BIN_FILE}.darwin.amd64,${MAIN_PROG})
	@$(call BUILD_MSG,Build MacOS amd64bit program - ${DST_DIR}/${BIN_FILE}.darwin.amd64)

# make ARGS="-v" run
run: build
	${RUN_BIN} $(ARGS)

check:
	${LINT_GUARD}
	@go fmt ./...
	@golangci-lint run

clean:
	@go clean
	@${RM_DIST}
