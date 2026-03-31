# Gera data/rh.csv, data/esocial.csv e data/gestao.csv — mesma quantidade por órgão e mesmo CPF nas 3 bases.
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$DataDir = Join-Path $Root "data"
New-Item -ItemType Directory -Force -Path $DataDir | Out-Null

$OrgCounts = @(
    @{ Sigla = "CGE"; Qtd = 199 },
    @{ Sigla = "PGE"; Qtd = 367 },
    @{ Sigla = "SEAD"; Qtd = 2692 },
    @{ Sigla = "ECONOMIA"; Qtd = 3108 },
    @{ Sigla = "DETRAN"; Qtd = 1274 },
    @{ Sigla = "SSP"; Qtd = 1586 },
    @{ Sigla = "CBM"; Qtd = 2675 },
    @{ Sigla = "DGPP"; Qtd = 3316 },
    @{ Sigla = "DGPC"; Qtd = 4738 },
    @{ Sigla = "SES"; Qtd = 7589 },
    @{ Sigla = "PM"; Qtd = 11647 },
    @{ Sigla = "SEDF"; Qtd = 38 },
    @{ Sigla = "FAPEG"; Qtd = 78 },
    @{ Sigla = "SECOM"; Qtd = 110 },
    @{ Sigla = "VICEGOV"; Qtd = 111 },
    @{ Sigla = "GOIAS TURISMO"; Qtd = 111 },
    @{ Sigla = "SERINT"; Qtd = 129 },
    @{ Sigla = "SEINFRA"; Qtd = 130 },
    @{ Sigla = "CASA CIVIL"; Qtd = 138 },
    @{ Sigla = "SEAPA"; Qtd = 152 },
    @{ Sigla = "SECTI"; Qtd = 178 },
    @{ Sigla = "JUCEG"; Qtd = 188 },
    @{ Sigla = "AGR"; Qtd = 192 },
    @{ Sigla = "SIC"; Qtd = 206 },
    @{ Sigla = "RETOMADA"; Qtd = 253 },
    @{ Sigla = "SECULT"; Qtd = 259 },
    @{ Sigla = "SEEL"; Qtd = 385 },
    @{ Sigla = "ABC"; Qtd = 390 },
    @{ Sigla = "SECAMI"; Qtd = 460 },
    @{ Sigla = "SEMAD"; Qtd = 522 },
    @{ Sigla = "SGG"; Qtd = 577 },
    @{ Sigla = "GOINFRA"; Qtd = 737 },
    @{ Sigla = "EMATER"; Qtd = 828 },
    @{ Sigla = "DPE-GO"; Qtd = 845 },
    @{ Sigla = "SEDS"; Qtd = 966 },
    @{ Sigla = "AGRODEFESA"; Qtd = 1004 },
    @{ Sigla = "UEG"; Qtd = 2001 },
    @{ Sigla = "SEDUC"; Qtd = 44981 },
    @{ Sigla = "GOIASPREV"; Qtd = 71101 }
)

$Nomes = "Ana Bruno Carla Daniel Eduarda Fabio Gabriela Hugo Isabela Joao Karina Lucas Mariana Nicolas Olivia Paulo Quiteria Rafael Sofia Thiago Andre Beatriz Camila Diego Elisa Felipe Gustavo Helena Igor Juliana Leonardo Marina Natalia Otavio Patricia Renata Samuel Tatiana Vinicius Yasmin" -split " "
$Sobrenomes = "Silva Santos Oliveira Souza Rodrigues Ferreira Alves Pereira Lima Gomes Ribeiro Carvalho Almeida Martins Rocha Dias Barbosa Cardoso Nogueira Moura Teixeira Freitas Barros Araujo Melo Monteiro Duarte Lopes Nunes Vieira Costa Ramos Correia Castro Fernandes Machado Pinto Cavalcanti" -split " "
$Cargos = "Analista Administrativo Tecnico de Nivel Superior Assistente Tecnico Contador Engenheiro Medico Enfermeiro Professor Motorista Agente de Fiscalizacao Advogado Auditor Tecnologista Assessor Pedagogo Psicologo Analista de TI Tecnico em Seguranca Operador Administrador" -split " "
$TipoVinculo = @("efetivo", "comissionado", "cedido")
$StatusRh = @("ativo", "afastado", "exonerado")
# Nomes distintos dos escalares ($sexo, $cor) — no PowerShell variáveis são case-insensitive e $Sexo sobrescreveria o array.
$ListaSexo = @("M", "F")
$EstadoCivil = @("solteiro", "solteira", "casado", "casada", "divorciado", "divorciada", "viuvo", "viuva", "uniao estavel")
$ListaCor = @("parda", "branca", "negra", "amarela")
$Ciclos = @("2023", "2024", "2025")

$script:Rng = [System.Random]::new(42)
$Rng = $script:Rng
$Inv = [System.Globalization.CultureInfo]::InvariantCulture

function Get-SlugMatricula([string]$Sigla) {
    return ($Sigla -replace " ", "" -replace "-", "")
}

function New-CpfValido {
    while ($true) {
        $n = @(0..8 | ForEach-Object { $script:Rng.Next(0, 10) })
        if (($n | Select-Object -Unique).Count -eq 1) { continue }
        $s1 = 0; for ($i = 0; $i -lt 9; $i++) { $s1 += (10 - $i) * $n[$i] }
        $s1 = $s1 % 11
        $d1 = if ($s1 -lt 2) { 0 } else { 11 - $s1 }
        $n10 = $n + @($d1)
        $s2 = 0; for ($i = 0; $i -lt 10; $i++) { $s2 += (11 - $i) * $n10[$i] }
        $s2 = $s2 % 11
        $d2 = if ($s2 -lt 2) { 0 } else { 11 - $s2 }
        return (($n10 + @($d2)) -join "")
    }
}

function New-UniqueCpfs([int]$Total) {
    $seen = [System.Collections.Generic.HashSet[string]]::new()
    $list = [System.Collections.Generic.List[string]]::new()
    while ($list.Count -lt $Total) {
        $c = New-CpfValido
        if ($seen.Add($c)) { [void]$list.Add($c) }
    }
    return $list
}

function Format-CpfRh([string]$Cpf) {
    return "{0}.{1}.{2}-{3}" -f $Cpf.Substring(0, 3), $Cpf.Substring(3, 3), $Cpf.Substring(6, 3), $Cpf.Substring(9, 2)
}

function Escape-Csv([string]$s) {
    if ($null -eq $s) { return '""' }
    if ($s.Contains('"') -or $s.Contains(',') -or $s.Contains("`n")) {
        return '"' + ($s -replace '"', '""') + '"'
    }
    return $s
}

$total = 0
foreach ($o in $OrgCounts) { $total += $o.Qtd }
if ($total -ne 166261) { throw "Total esperado 166261, obtido $total" }

$cpfs = New-UniqueCpfs $total
$cpfIdx = 0
$eventSeq = 0
$rows = [System.Collections.Generic.List[object]]::new()

foreach ($org in $OrgCounts) {
    $sigla = $org.Sigla
    $slug = Get-SlugMatricula $sigla
    for ($seq = 1; $seq -le $org.Qtd; $seq++) {
        $cpf = $cpfs[$cpfIdx]
        $cpfIdx++
        $eventSeq++
        $nome = "$($Nomes[$Rng.Next(0, $Nomes.Length)]) $($Sobrenomes[$Rng.Next(0, $Sobrenomes.Length)]) $($Sobrenomes[$Rng.Next(0, $Sobrenomes.Length)])"
        $birth = Get-Date -Year ($Rng.Next(1960, 2006)) -Month ($Rng.Next(1, 13)) -Day 1
        $birth = $birth.AddDays($Rng.Next(0, 28))
        $minAdm = $birth.AddYears(18)
        if ($minAdm -lt [datetime]"1995-01-01") { $minAdm = [datetime]"1995-01-01" }
        $endAdm = [datetime]"2025-12-31"
        $span = [math]::Max(1, ($endAdm - $minAdm).Days)
        $adm = $minAdm.AddDays($Rng.Next(0, $span))
        $r = $Rng.Next(0, 100)
        $status = if ($r -lt 88) { "ativo" } elseif ($r -lt 97) { "afastado" } else { "exonerado" }
        $saida = $null
        if ($status -eq "exonerado") {
            $saida = $adm.AddDays($Rng.Next(180, 4001))
            if ($saida -gt [datetime]::Today) { $saida = [datetime]::Today.AddDays(-30) }
        }
        $tvRoll = $Rng.Next(0, 100)
        $tipoV = if ($tvRoll -lt 82) { "efetivo" } elseif ($tvRoll -lt 96) { "comissionado" } else { "cedido" }
        $sal = [math]::Round($Rng.Next(280000, 1450000) / 100.0, 2)
        $lot = "Unidade $slug - Setor $($Rng.Next(1, 41))"
        $matRh = "RH-$slug-$('{0:D6}' -f $seq)"
        $matGes = "GES-$slug-$('{0:D6}' -f $seq)"
        $cargo = $Cargos[$Rng.Next(0, $Cargos.Length)]
        $idEv = "ESOC-$slug-$('{0:D8}' -f $eventSeq)"
        $sexo = $ListaSexo[$Rng.Next(0, $ListaSexo.Length)]
        $ec = $EstadoCivil[$Rng.Next(0, $EstadoCivil.Length)]
        $cor = $ListaCor[$Rng.Next(0, $ListaCor.Length)]
        $email = "servidor.$cpf@goias.gov.br"
        $tel = "(62) 9$($Rng.Next(1000, 10000))-$($Rng.Next(1000, 10000))"
        $aval = [math]::Round($Rng.Next(30, 51) / 10.0, 1)
        $ciclo = $Ciclos[$Rng.Next(0, $Ciclos.Length)]

        $rows.Add([pscustomobject]@{
            Nome = $nome
            Cpf = $cpf
            CpfRh = (Format-CpfRh $cpf)
            Birth = $birth
            Sigla = $sigla
            Adm = $adm
            Saida = $saida
            MatRh = $matRh
            MatGes = $matGes
            Cargo = $cargo
            TipoVinculo = $tipoV
            Salario = $sal
            Lotacao = $lot
            Status = $status
            IdEvento = $idEv
            Sexo = $sexo
            EstadoCivil = $ec
            Cor = $cor
            Email = $email
            Telefone = $tel
            Aval = $aval
            Ciclo = $ciclo
        })
    }
}

# $true = UTF-8 com BOM — Excel no Windows reconhece acentos (evita "OtÃ¡vio" em vez de "Otávio")
$utf8 = New-Object System.Text.UTF8Encoding $true
$rhPath = Join-Path $DataDir "rh.csv"
$esPath = Join-Path $DataDir "esocial.csv"
$gePath = Join-Path $DataDir "gestao.csv"

$wRh = New-Object System.IO.StreamWriter($rhPath, $false, $utf8)
$wEs = New-Object System.IO.StreamWriter($esPath, $false, $utf8)
$wGe = New-Object System.IO.StreamWriter($gePath, $false, $utf8)

$wRh.WriteLine("nome,cpf,data_nascimento,sigla_orgao,data_admissao,data_saida,matricula,cargo,tipo_vinculo,salario_base,lotacao,status_servidor")
$wEs.WriteLine("nome,cpf,data_nascimento,sigla_orgao,id_evento,tipo_evento,data_evento,codigo_categoria,indicador_retificacao,numero_recibo")
$wGe.WriteLine("nome,cpf,dataNascimento,orgao,dataEntrada,dataSaida,matricula,cargo,tipoVinculo,salario,unidadeLotacao,status,sexo,estadoCivil,cor,nacionalidade,email,telefone,avaliacaoDesempenho,cicloAvaliacao")

foreach ($p in $rows) {
    $isoBirth = $p.Birth.ToString("yyyy-MM-dd")
    $isoAdm = $p.Adm.ToString("yyyy-MM-dd")
    $isoSaida = if ($p.Saida) { $p.Saida.ToString("yyyy-MM-dd") } else { "" }
    $brBirth = $p.Birth.ToString("dd/MM/yyyy")
    $brAdm = $p.Adm.ToString("dd/MM/yyyy")
    $brSaida = if ($p.Saida) { $p.Saida.ToString("dd/MM/yyyy") } else { "" }
    $slug = Get-SlugMatricula $p.Sigla
    $recibo = "REC-$($p.Adm.Year)-$slug-$($p.Cpf.Substring(0, 6))"

    $wRh.WriteLine((
        @(
            (Escape-Csv $p.Nome),
            (Escape-Csv $p.CpfRh),
            $isoBirth,
            (Escape-Csv $p.Sigla),
            $isoAdm,
            $isoSaida,
            (Escape-Csv $p.MatRh),
            (Escape-Csv $p.Cargo),
            $p.TipoVinculo,
            $p.Salario.ToString($Inv),
            (Escape-Csv $p.Lotacao),
            $p.Status
        ) -join ","
    ))

    $wEs.WriteLine((
        @(
            (Escape-Csv $p.Nome),
            $p.Cpf,
            $isoBirth,
            (Escape-Csv $p.Sigla),
            (Escape-Csv $p.IdEvento),
            "admissao",
            $isoAdm,
            "301",
            "original",
            (Escape-Csv $recibo)
        ) -join ","
    ))

    $wGe.WriteLine((
        @(
            (Escape-Csv $p.Nome),
            $p.Cpf,
            $brBirth,
            (Escape-Csv $p.Sigla),
            $brAdm,
            $brSaida,
            (Escape-Csv $p.MatGes),
            (Escape-Csv $p.Cargo),
            $p.TipoVinculo,
            $p.Salario.ToString($Inv),
            (Escape-Csv $p.Lotacao),
            $p.Status,
            $p.Sexo,
            (Escape-Csv $p.EstadoCivil),
            $p.Cor,
            "Brasileira",
            (Escape-Csv $p.Email),
            (Escape-Csv $p.Telefone),
            $p.Aval.ToString($Inv),
            $p.Ciclo
        ) -join ","
    ))
}

$wRh.Close()
$wEs.Close()
$wGe.Close()

Write-Host "Gerados $total servidores em:"
Write-Host $rhPath
Write-Host $esPath
Write-Host $gePath
