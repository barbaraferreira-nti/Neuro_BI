@echo off

REM Ir para a pasta do projeto
cd /d C:\Users\Barbara\ProjetosPython

REM Ativar o ambiente virtual
call .venv\Scripts\activate

REM Executar o módulo
python -m Pipelines.Guru.atualizaDimGuru