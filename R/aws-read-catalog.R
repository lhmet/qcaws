#' Importa catálogo das Estações automaticas do INMET em um tibble.
#'
#' @param catalog_file caminho para o arquivo de catálogo das estações
#' meteorológicas do INMET.
#'
#' @return [tibble::tibble()] com dados limpos
#' @source https://portal.inmet.gov.br/paginas/catalogoaut e
#' https://portal.inmet.gov.br/paginas/catalogoman
#' @export

read_catalog <- function(catalog_file = here("input/CatalogoEstaçõesAutomáticas.csv")) {

  catalogo <- data.table::fread(catalog_file, header = TRUE) |>
    as_tibble() |>
    type_conv(locale = readr::locale(decimal_mark = ",")) |>
    magrittr::extract2("result") |>
    rename_with(tolower) |>
    mutate(dt_inicio_operacao = dmy(dt_inicio_operacao))

  novos_nomes <- c(
    nome = "dc_nome",
    estado = "sg_estado",
    lon = "vl_longitude",
    lat = "vl_latitude",
    alt = "vl_altitude",
    inicio = "dt_inicio_operacao",
    situacao = "cd_situacao",
    id = "cd_estacao"
  )

  catalogo <- catalogo |>
    rename(all_of(novos_nomes)) |>
    mutate(id = as.character(id))

  # 
  # catalogo |> filter(str_detect(nome, "VERDE"))
  # catalogo |> filter(str_detect(nome, "CAICO"))
  # catalogo |> filter(str_detect(nome, "PELOTAS"))
  # catalogo |> filter(str_detect(nome, "CACHOEIRA"))
  # catalogo |> filter(str_detect(nome, "CAMPOS"))
  # 
  
  # correcao dos nomes das estacoes devido a divergencia entre nomes
  # das automaticas e convencionais
  catalogo <- catalogo |>
    mutate(
      nome = stringr::str_trim(nome) |> toupper()
      # nome = data.table::fifelse(nome == "ARCOVERDE", "ARCO VERDE", nome),
      # nome = data.table::fifelse(nome == "SERIDO (CAICO)", "CAICO", nome),
      # nome = data.table::fifelse(nome == "SALVADOR (ONDINA)", "SALVADOR", nome),
      # nome = data.table::fifelse(nome == "S, G. DA CACHOEIRA", "SAO GABRIEL DA CACHOEIRA", nome),
      # nome = data.table::fifelse(nome == "CAPAO DO LEAO (PELOTAS)", "PELOTAS", nome)
    )


  

  # A770
  # SAO SIMAO
  #
  # A771
  # SAO PAULO - INTERLAGOS
  #
  catalogo
}
