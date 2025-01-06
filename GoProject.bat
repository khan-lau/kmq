@echo off

@REM echo Esc[32;40mhello world!Esc[0m

@REM color 0d

if not exist cmd (md cmd &echo 创建文件夹 cmd) else echo 检查文件夹 cmd
if not exist internal (md internal &echo 创建文件夹 internal) else echo 检查文件夹 internal
if not exist pkg (md pkg &echo 创建文件夹 pkg) else echo 检查文件夹 pkg

if not exist doc (md doc &echo 创建文档文件夹 doc) else echo 检查文档文件夹 doc
if not exist conf (md conf &echo 创建配置文件夹 conf) else echo 检查配置文件夹 conf
if not exist dist (md dist &echo 创建编译输出文件夹 dist) else echo 检查编译输出文件夹 dist
if not exist logs (md logs &echo 创建日志文件夹 logs) else echo 检查日志文件夹 logs
@REM pause

echo =======================================================================

@REM color 07

set need_proxy=1
set mod_file=go.mod
set tmp_file=temp.txt

if not exist %mod_file% (
    echo 模块文件不存在, 初始化模块 
    goto loop_input_proxy
) else (
    echo 检查模块文件 %mod_file%
)

:loop_get_project
    set /p file_line=<%mod_file%
    call:trim file_line, file_line

    if "%file_line%"=="" (
        more +1 %mod_file% >%tmp_file%
        
        call:isEmpty "%tmp_file%", is_file_empty
        setlocal enabledelayedexpansion

        if !is_file_empty! equ 1 (
            echo %mod_file% 为空, 重新创建模块
            endlocal
            del %tmp_file%
            goto loop_input_proxy
        ) else (
            @REM echo %mod_file% 不为空
            move /y %tmp_file% %mod_file% > nul
            endlocal
            goto loop_get_project
        )
    ) else (
        call:substring "%file_line%" 7 project_name
        echo =======================================================================

        setlocal enabledelayedexpansion
        if "!project_name!"=="" (
            echo get project name fault!
        ) else (
            echo PROJECT_NAME:!project_name!
            call:buildProject %need_proxy%
        )
        endlocal
        goto exit
    )

:end_loop_get_project


:loop_input_proxy
    set /p IS_PROXY=是否配置代理[Y/N/Q]:
    if "%IS_PROXY%"=="q" goto exit
    if "%IS_PROXY%"=="Q" goto exit

    if "%IS_PROXY%"=="n" set need_proxy=0
    if "%IS_PROXY%"=="N" set need_proxy=0

    if "%IS_PROXY%"=="y" set need_proxy=1
    if "%IS_PROXY%"=="Y" set need_proxy=1

    if need_proxy equ 1 (
        call:buildProject %need_proxy%
        goto end_loop_proxy
    ) else (
        goto end_loop_proxy
    )
    goto loop_input_proxy

:end_loop_proxy


:loop_input_project_name
    set /p PROJECT_NAME=请输入正确的模块名, 字母开头, 最短4个字符, 最长32个字符, 不含空格与不可见字符, [a-zA-Z][a-zA-Z0-9\/_]{3,31}:
    call:trim PROJECT_NAME PROJECT_NAME
    call:strlen PROJECT_NAME length

    rem echo string %PROJECT_NAME% length: %length%
    if "%PROJECT_NAME%"=="q" goto exit

    if %length% gtr 32 goto invalid
    if %length% lss 4 goto invalid

    if "%PROJECT_NAME%"=="exit" goto exit

    echo 您的模块名为:%PROJECT_NAME%
    goto end_input_project_name

    :invalid
        echo 您输入了错误的模块名, 请重新输入
        goto loop_input_project_name
:end_input_project_name

go mod init %PROJECT_NAME%
call:buildProject %need_proxy%

pause

goto exit


:strlen
    rem 接收两个参数，第一个是原始字符串变量，第二个是结果字符串变量
    setlocal enabledelayedexpansion
    set str=!%~1!
    set count=0
    :loop_strlen
    if not "%str%"=="" (
        set /a count+=1
        set str=%str:~1%
        goto loop_strlen
    )

    endlocal & set %~2=%count%
goto:eof


:trim
    rem 接收两个参数，第一个是原始字符串变量，第二个是结果字符串变量
    setlocal enabledelayedexpansion
    set str=!%~1!
    rem 循环去掉字符串前面的空格，直到遇到非空格字符为止
    :loop_trim0
    if "!str:~0,1!"==" " (
        set str=!str:~1!
        goto loop_trim0
    )
    rem 循环去掉字符串后面的空格，直到遇到非空格字符为止
    :loop_trim1
    if "!str:~-1!"==" " (
        set str=!str:~0,-1!
        goto loop_trim1
    )
    rem 返回trim后的字符串给结果变量，并退出函数
    endlocal & set %~2=%str%
goto :eof


:isEmpty
    @REM 接收两个参数，第一个是文件路径，第二个是结果字符串
    setlocal enabledelayedexpansion
    set file=%~1
    set empty=1
    for /f %%a in ('findstr /r /n "^" "%file%"') do (
        set empty=0
        goto :break
    )
    :break

    @REM echo empty:%empty%
    endlocal & set %~2=%empty%
goto :eof

:substring
    @REM 从str 的start位置, 截取到末尾
    @REM 接收3个参数，第一个是原始字符串变量，第二个是起始位, 第3个是结果字符串变量
    setlocal enabledelayedexpansion
    set src_str=%~1
    set start=%2
    call:strlen src_str length

    @REM echo src_st=%src_str% start=%start%, length=%length%
    set /a substr_len=%length%-%start%
    set str=!src_str:~%start%,%substr_len%!
    endlocal & set %~3=%str%
goto :eof


:buildProject
    setlocal enabledelayedexpansion
    @REM set http_proxy=socks5://127.0.0.1:51837
    @REM set https_proxy=socks5://127.0.0.1:51837

    @REM https://goproxy.cn
    @REM https://goproxy.io
    @REM https://mirrors.aliyun.com/goproxy

    @REM $env:GOPROXY="https://goproxy.cn";
    @REM $env:https_proxy="socks5://127.0.0.1:51837"
    go version
    echo 确保Golang版本不小于 1.13
    set GOPRIVATE=git.xxx.com
    echo GOPRIVATE=!GOPRIVATE!

    set need_proxy=%1
    if %need_proxy% equ 1 (
        set GOPROXY=https://goproxy.cn
        echo GOPROXY=!GOPROXY!
    ) else (
        echo need proxy: %need_proxy%
    )
    go mod tidy
    endlocal
goto :eof

echo on

:exit



