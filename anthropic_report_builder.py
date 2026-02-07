import json


def extrair_json_do_texto(texto):
    """
    Extrai o primeiro objeto JSON válido de um texto.
    Útil para quando o modelo retorna espaços/quebras de linha extras.
    """
    texto = texto.strip()
    decoder = json.JSONDecoder()
    for i, char in enumerate(texto):
        if char != "{":
            continue
        try:
            obj, end = decoder.raw_decode(texto[i:])
            # Aceita se após o JSON houver apenas espaços
            resto = texto[i + end:].strip()
            if resto == "":
                return obj
            return obj
        except json.JSONDecodeError:
            continue
    raise ValueError("Não foi possível extrair JSON válido da resposta do modelo.")


def _join_pt(lista):
    if not lista:
        return ""
    if len(lista) == 1:
        return lista[0]
    if len(lista) == 2:
        return f"{lista[0]} e {lista[1]}"
    return ", ".join(lista[:-1]) + f" e {lista[-1]}"


def _formatar_localizacao(local):
    mapa = {
        "RUL": "lobo superior direito",
        "RML": "lobo médio direito",
        "RLL": "lobo inferior direito",
        "LUL": "lobo superior esquerdo",
        "LLL": "lobo inferior esquerdo",
        "right_base": "base pulmonar direita",
        "left_base": "base pulmonar esquerda",
        "diffuse_bilateral": "distribuição bilateral difusa",
        "perihilar": "região peri-hilar",
    }
    return mapa.get(local, local)


def montar_laudo_a_partir_json(dados):
    linhas = [
        "Exame realizado no leito, apenas na incidência frontal."
    ]

    # Pulmões
    pulmoes = dados.get("lungs", {})
    achados_pulmonares = pulmoes.get("findings", []) or []
    edema = pulmoes.get("edema_signs", {}) or {}
    edema_presente = any([
        bool(edema.get("vascular_congestion")),
        bool(edema.get("interstitial_edema")),
        bool(edema.get("alveolar_edema")),
    ])

    if pulmoes.get("normal") is True and not achados_pulmonares and not edema_presente:
        linhas.append("Pulmões sem alterações grosseiras.")
    else:
        frases_pulmonares = []
        mapa_tipo = {
            "consolidation": "consolidação",
            "atelectasis": "atelectasia",
            "infiltration": "infiltrado",
            "nodule": "nódulo",
            "mass": "massa",
            "cavitation": "cavitação",
        }

        # Regra: se houver consolidação + atelectasia na mesma localização,
        # consolidar em "consolidação/atelectasia em <local>".
        tipos_por_local = {}
        for achado in achados_pulmonares:
            local_raw = achado.get("location")
            tipo_raw = achado.get("type")
            if not local_raw or not tipo_raw:
                continue
            tipos_por_local.setdefault(local_raw, set()).add(tipo_raw)

        # Regra: se houver o mesmo tipo em right_base + left_base,
        # consolidar em "<tipo> em bases".
        tipos_bases_direita = tipos_por_local.get("right_base", set())
        tipos_bases_esquerda = tipos_por_local.get("left_base", set())
        tipos_em_bases = tipos_bases_direita.intersection(tipos_bases_esquerda)
        for tipo_base in sorted(tipos_em_bases):
            tipo_txt = mapa_tipo.get(tipo_base, tipo_base)
            frases_pulmonares.append(f"{tipo_txt} em bases")

        locais_combinados = set()
        for local_raw, tipos in tipos_por_local.items():
            if "consolidation" in tipos and "atelectasis" in tipos:
                local = _formatar_localizacao(local_raw)
                frases_pulmonares.append(f"consolidação/atelectasia em {local}")
                locais_combinados.add(local_raw)

        for achado in achados_pulmonares:
            tipo_raw = achado.get("type")
            local_raw = achado.get("location")
            if not tipo_raw or not local_raw:
                continue
            if local_raw in {"right_base", "left_base"} and tipo_raw in tipos_em_bases:
                continue
            if local_raw in locais_combinados and tipo_raw in {"consolidation", "atelectasis"}:
                continue
            tipo = mapa_tipo.get(tipo_raw, tipo_raw)
            local = _formatar_localizacao(local_raw)
            frases_pulmonares.append(f"{tipo} em {local}")

        sinais_edema = []
        if edema.get("vascular_congestion"):
            sinais_edema.append("congestão vascular")
        if edema.get("interstitial_edema"):
            sinais_edema.append("edema intersticial")
        if edema.get("alveolar_edema"):
            sinais_edema.append("edema alveolar")
        if sinais_edema:
            frases_pulmonares.append(_join_pt(sinais_edema))

        if frases_pulmonares:
            linhas.append("Pulmões com " + _join_pt(frases_pulmonares) + ".")
        else:
            linhas.append("Pulmões sem alterações grosseiras.")

    # Pleura
    pleura = dados.get("pleura", {})
    derrame = (pleura.get("pleural_effusion") or {})
    derrame_direito = bool(derrame.get("right"))
    derrame_esquerdo = bool(derrame.get("left"))

    if not derrame_direito and not derrame_esquerdo:
        linhas.append("Ausência de derrame pleural identificável.")
    elif derrame_direito and derrame_esquerdo:
        linhas.append("Derrame pleural bilateral.")
    elif derrame_direito:
        linhas.append("Derrame pleural à direita.")
    else:
        linhas.append("Derrame pleural à esquerda.")

    pneumotorax = (pleura.get("pneumothorax") or {})
    ptx_direito = bool(pneumotorax.get("right"))
    ptx_esquerdo = bool(pneumotorax.get("left"))
    if ptx_direito and ptx_esquerdo:
        linhas.append("Pneumotórax bilateral.")
    elif ptx_direito:
        linhas.append("Pneumotórax à direita.")
    elif ptx_esquerdo:
        linhas.append("Pneumotórax à esquerda.")

    # Área cardíaca
    mediastino = dados.get("mediastinum", {})
    if mediastino.get("cardiomegaly"):
        linhas.append("Área cardíaca aumentada.")
    else:
        linhas.append("Área cardíaca normal.")

    # Dispositivos
    dispositivos = dados.get("support_devices", {})
    linhas_dispositivos = []

    ett = dispositivos.get("endotracheal_tube", {})
    if ett.get("present"):
        linhas_dispositivos.append("Tubo endotraqueal presente.")

    trq = dispositivos.get("tracheostomy", {})
    if trq.get("present"):
        linhas_dispositivos.append("Traqueostomia presente.")

    cvc = dispositivos.get("central_venous_catheters", []) or []
    tipo_cateter_map = {
        "CVC": "Cateter venoso central",
        "PICC": "Cateter venoso central de inserção periférica pelo membro superior",
        "PIC": "Cateter venoso central de inserção periférica pelo membro superior",
        "Dialysis_Catheter": "Cateter de diálise",
        "Port-a-cath": "Port-a-cath",
    }
    lado_map = {
        "right": "direito",
        "left": "esquerdo",
    }
    lado_map_a = {
        "right": "direita",
        "left": "esquerda",
    }
    ponta_map = {
        "SVC": "na veia cava superior",
        "subclavian_vein": "na veia subclávia",
        "jugular_vein": "na veia jugular",
        "misplaced": "em posição anômala",
        "unknown": "com extremidade distal não visibilizada",
        "RA": "no átrio direito",
    }
    for cateter in cvc:
        tipo_raw = cateter.get("type")
        lado_raw = cateter.get("side")
        ponta_raw = cateter.get("tip_location")

        tipo_txt = tipo_cateter_map.get(tipo_raw, "Cateter venoso")
        lado_txt = lado_map.get(lado_raw)
        lado_txt_a = lado_map_a.get(lado_raw)
        ponta_txt = ponta_map.get(ponta_raw)

        if tipo_raw in {"PICC", "PIC"} and lado_txt:
            base = f"{tipo_txt} {lado_txt}"
        elif lado_txt_a:
            base = f"{tipo_txt} à {lado_txt_a}"
        else:
            base = tipo_txt

        if ponta_txt:
            if ponta_raw == "unknown":
                linhas_dispositivos.append(f"{base}, {ponta_txt}.")
            else:
                linhas_dispositivos.append(f"{base} com extremidade distal {ponta_txt}.")
        else:
            linhas_dispositivos.append(f"{base}.")

    sng = dispositivos.get("nasogastric_tube", {})
    if sng.get("present"):
        ponta_sng = sng.get("tip_location")
        ponta_sng_norm = str(ponta_sng or "").strip().lower()
        if ponta_sng_norm == "stomach":
            linhas_dispositivos.append("Sonda transesofágica com extremidade distal no estômago.")
        elif ponta_sng_norm == "duodenum":
            linhas_dispositivos.append("Sonda transesofágica com extremidade distal no duodeno.")
        elif ponta_sng_norm == "esophagus":
            linhas_dispositivos.append("Sonda transesofágica com extremidade distal no esôfago.")
        elif ponta_sng_norm == "bronchus":
            linhas_dispositivos.append("Sonda transesofágica com extremidade distal em brônquio.")
        elif (
            ponta_sng_norm == "right_bronchus"
            or ponta_sng_norm == "bronchus_right"
            or ponta_sng_norm == "right bronchus"
        ):
            linhas_dispositivos.append("Sonda transesofágica com extremidade distal em brônquio direito.")
        elif (
            ponta_sng_norm == "left_bronchus"
            or ponta_sng_norm == "bronchus_left"
            or ponta_sng_norm == "left bronchus"
        ):
            linhas_dispositivos.append("Sonda transesofágica com extremidade distal em brônquio esquerdo.")
        else:
            linhas_dispositivos.append("Sonda transesofágica presente.")

    drenos = dispositivos.get("chest_drains", []) or []
    for dreno in drenos:
        lado_dreno = dreno.get("side")
        if lado_dreno == "right":
            linhas_dispositivos.append("Dreno torácico à direita.")
        elif lado_dreno == "left":
            linhas_dispositivos.append("Dreno torácico à esquerda.")
        else:
            linhas_dispositivos.append("Dreno torácico presente.")

    # Só reporta eletrodos de monitorização quando vier campo explícito no JSON.
    monitorizacao = dispositivos.get("monitoring_electrodes")
    if monitorizacao is True:
        linhas_dispositivos.append("Polos de eletrodos de monitorização na parede torácica.")
    elif isinstance(monitorizacao, dict) and monitorizacao.get("present") is True:
        linhas_dispositivos.append("Polos de eletrodos de monitorização na parede torácica.")

    if linhas_dispositivos:
        linhas.extend(linhas_dispositivos)

    return "\n".join(linhas)
