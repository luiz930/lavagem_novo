# Roadmap de Produto

## Fase 1 - Estabilizacao

- modularizacao inicial do backend em `core/`
- migrations versionadas
- seguranca base
- testes minimos
- documentacao de setup e deploy

## Fase 2 - Produto

- multiempresa via `empresa_id`
- licenciamento via `licencas`
- telemetria via `telemetria_eventos`
- white-label basico em `configuracao_empresa`
- storage padronizado por provider

## Fase 3 - Venda

- deploy padronizado
- plano/licenca por empresa
- suporte e diagnostico
- branding e dominios personalizados

## Corte atual entregue

- painel admin de empresas e troca de empresa ativa
- licenciamento por plano/status com limites de usuarios e atendimentos
- diagnostico de ambiente com checklist de producao
- base para bloqueio operacional quando a licenca vence ou excede limites

## Proximo corte tecnico recomendado

1. separar rotas por blueprint
2. isolar SQL por dominio
3. aplicar filtros de `empresa_id` nas consultas
4. mover uploads para provider unico
5. fechar CSRF em todos os formularios

## Corte de release atual

- o produto pode sair do beta quando a porta de saida de `RELEASE_1_0_0.md` estiver verde no ambiente real
- o bump para `1.0.0` deve ser o ultimo passo do corte, nao o primeiro
