# Release 1.0.0

## Objetivo

Publicar o primeiro corte estavel do produto apenas depois que codigo, ambiente e operacao estiverem validados no mesmo estado que ira para producao.

## Porta de saida obrigatoria

- `FLASK_SECRET_KEY` definida no ambiente de producao
- `CSRF_PROTECTION=1`
- `SESSION_COOKIE_SECURE=1`
- HTTPS ativo no dominio final
- banco online conectado em modo `postgres`
- backup novo gerado no dia da publicacao
- backup validado antes da publicacao
- smoke test manual concluido nos fluxos principais
- checklist de `/diagnostico` sem pendencias obrigatorias

## Validacao tecnica

Executar antes do deploy:

```powershell
python -m py_compile app.py
python -m unittest discover -s tests
```

Confirmar no ambiente real:

1. abrir `/diagnostico`
2. validar `Chave secreta configurada`, `CSRF ativo`, `Cookie seguro`, `Banco online`, `HTTPS ativo` e `Backup com arquivo recente`
3. gerar backup manual em `/configuracoes/banco`
4. validar o backup recem-gerado

## Smoke test manual

1. login
2. painel operacional
3. busca/cadastro de cliente
4. abertura e atualizacao de atendimento
5. historico e edicao de atendimento
6. financeiro
7. configuracoes
8. AutoSuporte

## Corte final de versao

Depois da porta de saida ficar verde:

1. atualizar `VERSAO_SISTEMA_PADRAO` em `app.py` para `1.0.0`
2. atualizar a versao exibida em `/configuracoes/sistema` para `1.0.0`
3. revisar `/changelog`
4. rodar novamente os testes
5. publicar

## Nao publicar se

- houver item obrigatorio vermelho em `/diagnostico`
- o backup mais recente nao tiver sido gerado no dia da publicacao
- `SESSION_COOKIE_SECURE` estiver desligado no dominio HTTPS
- `FLASK_SECRET_KEY` estiver vazia no ambiente real
- algum fluxo principal falhar no smoke test
