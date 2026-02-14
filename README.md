1. Limpeza dos Arquivos Locais (Importante!)
Antes de rodar os comandos, vá na pasta do seu projeto em leitorxml/core/migrations/ e apague todos os arquivos .py (como 0001_initial.py), EXCETO o arquivo __init__.py.

Isso garante que o Django crie uma migração nova contendo a correção da classe Item que fizemos agora.

2. Comandos para criar tudo
Abra o terminal, ative seu ambiente virtual (venv) e rode um comando por vez:

A. Gerar o arquivo de configuração (Receita)
PowerShell
python manage.py makemigrations
Deve aparecer: Create model Nfe, Create model Cte, Create model Item, etc.

B. Criar as tabelas no Banco de Dados (Cozinhar)
PowerShell
python manage.py migrate
Deve aparecer uma lista de OK (auth, admin, contenttypes, sessions, core).

C. Criar seu Usuário Administrador
PowerShell
python manage.py createsuperuser
Usuário: (Digite um nome, ex: admin)

E-mail: (Pode deixar em branco, dê Enter)

Senha: (Digite uma senha. Atenção: O cursor não vai andar enquanto você digita, é normal. Digite e dê Enter).

Confirmação: (Digite a senha de novo).

D. Rodar o Sistema
PowerShell
python manage.py runserver
Verificação Final
Acesse http://127.0.0.1:8000.

Logue com o usuário e senha que criou.

Verifique se as tabelas (Nfes, Ctes, Items) aparecem lá.

Vá para http://127.0.0.1:8000/upload/ e teste o upload.
# leitorfiscalmaster
