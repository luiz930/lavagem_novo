# Wagen Estetica Automotiva

Sistema Flask para operacao de estetica automotiva com:

- cadastro de clientes e veiculos
- painel operacional de atendimento
- checklist e fotos por atendimento
- retornos comerciais
- financeiro, orcamentos e notas
- backup, sincronizacao e banco online

## Stack

- Python / Flask
- SQLite local + PostgreSQL/Supabase
- Jinja templates
- Google Drive para backup opcional

## Fundacao de produto

Esta base agora possui:

- migrations versionadas em `schema_migrations`
- base de multiempresa em `empresas` e `empresa_id`
- tabela de licencas em `licencas`
- telemetria em `telemetria_eventos`
- configuracoes de white-label em `configuracao_empresa`
- testes minimos em `tests/`
- CI em `.github/workflows/ci.yml`

## Arquivos de referencia

- `INSTALL.md`
- `DEPLOY.md`
- `PRODUCT_ROADMAP.md`
- `RELEASE_1_0_0.md`
