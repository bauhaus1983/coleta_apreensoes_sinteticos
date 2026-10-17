# Coletor de apreensões — drogas sintéticas e precursores (PF, PRF, Receita Federal)

Alimenta o painel “Apreensões de Drogas Sintéticas no Brasil — 2026”. Gera `dados/apreensoes.json` no mesmo formato do painel, mais um CSV e um relatório por execução.

## Modo sem custo (padrão no GitHub)
O agendamento roda `python coleta.py --so-links`: descobre e filtra as notícias **sem usar nenhuma API** e acumula as candidatas em `links/pendentes.md`. Na segunda-feira, cole esse arquivo no chat do Claude e peça “extraia as apreensões destes links e atualize o painel”. Depois, rode a ação “limpar pendentes” para começar uma nova lista. Links já entregues nunca voltam.

## Modo completo (opcional, usa a API paga)
1. **Descobre** notícias novas: listagem e busca interna da PF, busca da Receita Federal, API de busca da PRF (a página da PRF é montada por JavaScript), Sindireceita (aduana) e imprensa nacional/local via Google Notícias.
2. **Filtra barato**: só segue adiante o que cita uma substância monitorada e um verbo de apreensão (na imprensa, também PF/PRF/Receita). Incinerações e balanços são descartados.
3. **Lê** apenas as candidatas e **extrai** os campos com a API do Claude (data, cidade/UF, modal, instituição, substâncias, quantidades, presos, armas, dinheiro).
4. **Deduplica**: mesma substância + mesma UF + cidade semelhante + data ±2 dias + quantidade compatível (±20%). A versão oficial prevalece; as demais viram fontes secundárias e completam campos vazios.
5. **Grava** a base, o CSV (`;`, abre direto no Excel) e `relatorios/coleta_AAAA-MM-DD_HHMM.md` (novas, atualizadas, para revisar).

URLs já lidas ficam em `dados/estado.json` e não são lidas de novo. Cada execução cobre desde a anterior, com 3 dias de folga.

## Instalação
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export ANTHROPIC_API_KEY=...      # sem chave: extração heurística, tudo marcado "revisar"
```

## Uso
```bash
python coleta.py --so-links           # sem custo: só a lista de candidatas (links/pendentes.md)
python coleta.py --limpar-pendentes   # arquiva a lista já processada
python coleta.py --desde 2026-01-01   # modo completo: carga retrospectiva do ano
python coleta.py                      # rotina: incremental
python coleta.py --seco -v            # testa sem gravar, com log detalhado
python coleta.py --fontes pf,prf      # só algumas fontes (pf, prf, receita, sindireceita, imprensa)
```
Modelo: `claude-haiku-5-5` (troque com `MODELO_CLAUDE=...`). Só as notícias que passam no filtro geram chamadas.

## Agendamento (seg, qua e sex às 7h)
- **GitHub Actions** (`.github/workflows/coleta.yml`): roda o modo sem custo. Na aba Actions → “Run workflow” há três ações: coletar links, limpar pendentes e carga do ano.
- **cron** (Linux/Mac, horário local de Brasília):
  `0 7 * * 1,3,5 cd /caminho/coletor_apreensoes && .venv/bin/python coleta.py >> coleta.log 2>&1`

## Atualizar o painel
O script não publica no painel. Na segunda-feira, envie `dados/apreensoes.json` ao Claude na conversa do projeto e peça “atualize o painel com este arquivo”. Antes, confira a seção “Para revisar” do relatório.

## Pontos de atenção
- **PRF**: usa a API de busca que alimenta a página. Se ela mudar, o script avisa e tenta o Playwright (opcional). Sem os dois, a PRF fica coberta só pela imprensa.
- **PF**: a listagem do site só volta cerca de três meses; meses anteriores vêm da busca interna por termo.
- **Imprensa**: quando o texto completo não abre, usa título e resumo e marca para revisão.
- Os totais refletem o que os órgãos e a imprensa divulgam: são indicadores, não estatística oficial.
