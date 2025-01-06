@echo off

@REM echo Esc[32;40mhello world!Esc[0m

@REM color 0d

if not exist cmd (md cmd &echo �����ļ��� cmd) else echo ����ļ��� cmd
if not exist internal (md internal &echo �����ļ��� internal) else echo ����ļ��� internal
if not exist pkg (md pkg &echo �����ļ��� pkg) else echo ����ļ��� pkg

if not exist doc (md doc &echo �����ĵ��ļ��� doc) else echo ����ĵ��ļ��� doc
if not exist conf (md conf &echo ���������ļ��� conf) else echo ��������ļ��� conf
if not exist dist (md dist &echo ������������ļ��� dist) else echo ����������ļ��� dist
if not exist logs (md logs &echo ������־�ļ��� logs) else echo �����־�ļ��� logs
@REM pause

echo =======================================================================

@REM color 07

set need_proxy=1
set mod_file=go.mod
set tmp_file=temp.txt

if not exist %mod_file% (
    echo ģ���ļ�������, ��ʼ��ģ�� 
    goto loop_input_proxy
) else (
    echo ���ģ���ļ� %mod_file%
)

:loop_get_project
    set /p file_line=<%mod_file%
    call:trim file_line, file_line

    if "%file_line%"=="" (
        more +1 %mod_file% >%tmp_file%
        
        call:isEmpty "%tmp_file%", is_file_empty
        setlocal enabledelayedexpansion

        if !is_file_empty! equ 1 (
            echo %mod_file% Ϊ��, ���´���ģ��
            endlocal
            del %tmp_file%
            goto loop_input_proxy
        ) else (
            @REM echo %mod_file% ��Ϊ��
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
    set /p IS_PROXY=�Ƿ����ô���[Y/N/Q]:
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
    set /p PROJECT_NAME=��������ȷ��ģ����, ��ĸ��ͷ, ���4���ַ�, �32���ַ�, �����ո��벻�ɼ��ַ�, [a-zA-Z][a-zA-Z0-9\/_]{3,31}:
    call:trim PROJECT_NAME PROJECT_NAME
    call:strlen PROJECT_NAME length

    rem echo string %PROJECT_NAME% length: %length%
    if "%PROJECT_NAME%"=="q" goto exit

    if %length% gtr 32 goto invalid
    if %length% lss 4 goto invalid

    if "%PROJECT_NAME%"=="exit" goto exit

    echo ����ģ����Ϊ:%PROJECT_NAME%
    goto end_input_project_name

    :invalid
        echo �������˴����ģ����, ����������
        goto loop_input_project_name
:end_input_project_name

go mod init %PROJECT_NAME%
call:buildProject %need_proxy%

pause

goto exit


:strlen
    rem ����������������һ����ԭʼ�ַ����������ڶ����ǽ���ַ�������
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
    rem ����������������һ����ԭʼ�ַ����������ڶ����ǽ���ַ�������
    setlocal enabledelayedexpansion
    set str=!%~1!
    rem ѭ��ȥ���ַ���ǰ��Ŀո�ֱ�������ǿո��ַ�Ϊֹ
    :loop_trim0
    if "!str:~0,1!"==" " (
        set str=!str:~1!
        goto loop_trim0
    )
    rem ѭ��ȥ���ַ�������Ŀո�ֱ�������ǿո��ַ�Ϊֹ
    :loop_trim1
    if "!str:~-1!"==" " (
        set str=!str:~0,-1!
        goto loop_trim1
    )
    rem ����trim����ַ�����������������˳�����
    endlocal & set %~2=%str%
goto :eof


:isEmpty
    @REM ����������������һ�����ļ�·�����ڶ����ǽ���ַ���
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
    @REM ��str ��startλ��, ��ȡ��ĩβ
    @REM ����3����������һ����ԭʼ�ַ����������ڶ�������ʼλ, ��3���ǽ���ַ�������
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
    echo ȷ��Golang�汾��С�� 1.13
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



