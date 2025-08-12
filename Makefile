.PHONY: all build clean run

DST_DIR=dist
PWD=

BIN_FILE=kmq
MAIN_PROG= cmd/main_helper.go \
		cmd/main_service.go \
		cmd/main.go

Version=0.1.2
Author=Liu Kun

DEBUG=-w -s
param=-X main.BuildVersion=${Version} -X \"main.BuildTime=${BuildDate}\" -X \"main.BuildPerson=${Author}\" -X \"main.BuildName=${BIN_FILE}\"


# 全局设置 SHELL
ifeq ($(OS),Windows_NT)
    SHELL=cmd.exe
else
    SHELL=/bin/sh
endif

ifeq ($(OS),Windows_NT)
	uname_S=Windows

    # 判断是否存在uname命令
	tmp_uname=$(shell cmd /C "where /Q uname && echo YES||echo NO")
	ifeq ($(tmp_uname),YES)
		term_S=Windows_Mingw
	endif
	tmp_lint=$(shell cmd /C "where /Q golangci-lint && echo YES||echo NO")
	BuildDate=$(shell powershell -Command "Get-Date -Format 'yyyy-MM-dd HH:mm:ss'")
	PWD=$(shell cmd /C "chdir")
else
	uname_S=$(shell uname -s)
	BuildDate=$(shell date +"%F %T")
	PWD=$(shell pwd)
	tmp_lint=$(shell  which golangci-lint > /dev/null 2>&1 && echo YES || echo NO)
endif

# $(warning OS: $(OS) - ${uname_S} , ${param}, ${PWD})
# $(warning VAR: $(RM), $(TARGET_OS), tmp_uname: ${tmp_uname},  term_S: ${term_S}, tmp_lint: ${tmp_lint})

all: build

build: win linux
	
win:
ifeq ($(uname_S), Windows)
	@cmd /C 'set CGO_ENABLED=1&&set GOOS=windows&&go build -v -ldflags '${DEBUG} ${param}' -o ${DST_DIR}/${BIN_FILE}.exe  ${MAIN_PROG}'
	@powershell -Command "Write-Host \"Build windows 64bit program - ${DST_DIR}/${BIN_FILE}.exe\" -ForegroundColor green"
endif
ifeq ($(uname_S), Linux)
	@export CGO_ENABLED=1; export GOOS=windows; go build -v -ldflags "${DEBUG} ${param}" -o ${DST_DIR}/${BIN_FILE}.exe  ${MAIN_PROG}
	@echo -e "\033[0;32m Build windows 64bit program - ${DST_DIR}/${BIN_FILE}.exe\033[0m"
endif

linux:
ifeq ($(uname_S), Windows)
	@cmd /C 'set CGO_ENABLED=0&&set GOOS=linux&&go build -v -ldflags '${DEBUG} ${param}' -o ${DST_DIR}/${BIN_FILE}  ${MAIN_PROG}'
	@powershell -Command "Write-Host \"Build linux 64bit program - ${DST_DIR}/${BIN_FILE}\" -ForegroundColor green"

endif
ifeq ($(uname_S), Linux)
	@export CGO_ENABLED=0; export GOOS=linux; go build -v -ldflags "${DEBUG} ${param}" -o ${DST_DIR}/${BIN_FILE}  ${MAIN_PROG}
	@echo -e "\033[0;32m Build linux 64bit program - ${DST_DIR}/${BIN_FILE}\033[0m"

endif


armlinux:
ifeq ($(uname_S), Windows)
	@cmd /C 'set CGO_ENABLED=0&&set GOOS=linux&&set GOARCH=arm64&&go build -v -ldflags '${DEBUG} ${param}' -o ${DST_DIR}/${BIN_FILE}.arm64  ${MAIN_PROG}'
	@powershell -Command "Write-Host \"Build linux 64bit program - ${DST_DIR}/${BIN_FILE}.arm64\" -ForegroundColor green"

endif
ifeq ($(uname_S), Linux)
	@export CGO_ENABLED=0; export GOOS=linux; export GOARCH=arm64; go build -v -ldflags "${DEBUG} ${param}" -o ${DST_DIR}/${BIN_FILE}.arm64  ${MAIN_PROG}
	@echo -e "\033[0;32m Build linux 64bit program - ${DST_DIR}/${BIN_FILE}.arm64\033[0m"

endif

# make ARGS="-v" run
run:
ifeq ($(uname_S), Windows)
	${DST_DIR}/${BIN_FILE}.exe $(ARGS)
endif
ifeq ($(uname_S), Linux)
	${DST_DIR}/${BIN_FILE} $(ARGS)
endif


check:
# 格式化代码
ifeq ($(uname_S), Windows)

# 检查是否已安装 golangci-lint
ifeq ($(term_S), Windows_Mingw)
ifeq (${tmp_lint}, NO)
	$(error golangci-lint not found!  try run 'go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest' at mingw .${tmp_lint})
endif
else
ifeq (${tmp_lint}, NO)
	$(error golangci-lint not found!  try run 'go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest' at cmd .${tmp_lint})
endif
endif

ifeq ($(term_S), Windows_Mingw)
	@rm -f NUL
	@find ./ -name '*.go' -exec go fmt {} \;
else
	@powershell -Command "Get-ChildItem -Path \"${PWD}\" -recurse *.go |ForEach-Object {go fmt $$_.FullName}"
endif

endif

ifeq ($(uname_S), Linux)
ifeq (tmp_lint, NO)
	$(error golangci-lint not found!  try run 'go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest' at bash)
endif
	@find ./ -name '*.go' -exec go fmt {} \;
endif

# 语法检查
# go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@golangci-lint run


clean:
	@go clean
ifeq ($(uname_S), Windows)
	@cmd /C 'del /F /Q ${DST_DIR}\\*'
endif
ifeq ($(uname_S), Linux)
	@rm --force ${DST_DIR}/*
endif
