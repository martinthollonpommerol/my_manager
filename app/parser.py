"""Parse a single email.message.Message into a structured dict.

This module is pure Python — no I/O, no database.  All filtering logic
(calendar invites, empty messages) is also applied here so the importer
only receives messages that are ready to insert.
"""

from __future__ import annotations  # active les annotations de type en mode chaîne (pas d'évaluation à l'import)

import email.header   # décodage des en-têtes RFC 2047 (encodage MIME des caractères non-ASCII)
import email.message  # type de base pour un message email Python
import email.utils    # utilitaires : parsing d'adresses, de dates RFC 2822
import logging        # journalisation des messages ignorés
import re             # expressions régulières pour extraire les message-ID entre chevrons
from dataclasses import dataclass, field  # dataclass : classe de données sans boilerplate ; field : valeur par défaut mutable
from datetime import datetime, timezone   # datetime : représentation d'une date/heure ; timezone : normalisation en UTC
from typing import Optional               # Optional[X] = X | None, pour les champs pouvant être absents

logger = logging.getLogger(__name__)  # logger nommé "app.parser"

# Regex to extract all <message-id> tokens from a header value
_MESSAGE_ID_RE = re.compile(r"<([^>]+)>")  # capture tout ce qui est entre < et > (ex: <abc@mail.example.com>)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass                           # génère automatiquement __init__, __repr__, __eq__
class Recipient:
    recipient_type: str              # type de destinataire : "to", "cc" ou "bcc"
    address: str                     # adresse email normalisée en minuscules
    display_name: Optional[str]      # nom affiché (ex: "Alice Dupont"), peut être absent


@dataclass
class Attachment:
    content_type: str           # type MIME de la pièce jointe (ex: "application/pdf")
    filename: Optional[str]     # nom du fichier, absent si non spécifié dans les en-têtes
    content_id: Optional[str]   # Content-ID pour les images inline référencées dans le HTML (cid:…)
    size_bytes: int             # taille en octets du contenu décodé
    data: bytes                 # contenu brut de la pièce jointe, stocké tel quel en BYTEA


@dataclass
class ParsedMessage:
    message_id_header: str              # valeur nettoyée du header Message-ID, clé de déduplication
    in_reply_to: Optional[str]          # message-ID du message auquel celui-ci répond directement
    references_header: list[str]        # liste ordonnée de tous les message-IDs du header References
    subject: Optional[str]             # objet du message, décodé depuis RFC 2047 si nécessaire
    sent_at: Optional[datetime]         # date d'envoi normalisée en UTC, None si absente ou invalide
    from_address: str                   # adresse email de l'expéditeur, normalisée en minuscules
    from_name: Optional[str]            # nom affiché de l'expéditeur, peut être absent
    body_text: Optional[str]            # contenu de la partie text/plain, None si absente
    body_html: Optional[str]            # contenu de la partie text/html, None si absente
    raw_headers: dict[str, str]         # tous les en-têtes bruts décodés, pour stockage JSONB
    recipients: list[Recipient] = field(default_factory=list)   # liste des destinataires To/Cc/Bcc
    attachments: list[Attachment] = field(default_factory=list) # liste des pièces jointes


class SkipMessage(Exception):
    """Raised when a message should not be imported."""  # signal d'abandon propre, attrapé par l'importer


# ---------------------------------------------------------------------------
# Header decoding helpers
# ---------------------------------------------------------------------------


def _decode_header(value: Optional[str]) -> Optional[str]:
    """Decode an RFC 2047 encoded header value to a plain string."""
    if value is None:           # entrée None → sortie None, évite de traiter les en-têtes absents
        return None
    parts = email.header.decode_header(value)  # découpe la valeur en fragments (texte, charset), chacun potentiellement encodé
    decoded_parts: list[str] = []              # accumulateur de fragments déjà décodés en str
    for raw, charset in parts:                 # itère sur chaque fragment et son encodage déclaré
        if isinstance(raw, bytes):             # fragment encodé (base64 ou quoted-printable)
            try:
                decoded_parts.append(raw.decode(charset or "utf-8", errors="replace"))  # décode avec le charset annoncé, utf-8 par défaut
            except (LookupError, UnicodeDecodeError):                                    # charset inconnu ou données corrompues
                decoded_parts.append(raw.decode("latin-1", errors="replace"))           # repli sur latin-1, qui accepte tout octet
        else:
            decoded_parts.append(raw)          # fragment déjà str (partie ASCII non encodée)
    return "".join(decoded_parts).strip()      # réassemble les fragments et supprime les espaces en bordure


def _parse_date(value: Optional[str]) -> Optional[datetime]:
    """Parse an RFC 2822 date string.  Returns None on any failure."""
    if not value:       # en-tête Date absent ou vide : retour None plutôt qu'une erreur
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(value)  # parse le format RFC 2822 (ex: "Thu, 1 Apr 2026 12:00:00 +0200")
        # Normalise to UTC if the date is offset-aware.
        if parsed.tzinfo is not None:                       # date avec fuseau horaire déclaré
            return parsed.astimezone(timezone.utc)          # convertit en UTC pour uniformité en base
        # Treat naive datetimes as UTC.
        return parsed.replace(tzinfo=timezone.utc)          # date sans fuseau : on suppose UTC plutôt que de la rejeter
    except Exception:   # format de date invalide ou non supporté : ne pas faire échouer tout l'import
        return None


def _parse_addresses(value: Optional[str]) -> list[tuple[Optional[str], str]]:
    """Return a list of (display_name, address) from a header value."""
    if not value:       # en-tête absent ou vide : aucune adresse à extraire
        return []
    pairs = email.utils.getaddresses([value])   # parse une liste d'adresses RFC 2822, gère les virgules et les noms affichés
    result: list[tuple[Optional[str], str]] = []  # liste de (nom, adresse) à retourner
    for name, addr in pairs:                    # itère sur chaque paire nom/adresse extraite
        addr = addr.strip().lower()             # normalise l'adresse : supprime les espaces et met en minuscules
        if not addr:                            # paire mal formée sans adresse valide : on l'ignore
            continue
        result.append((_decode_header(name) or None, addr))  # décode le nom (peut être RFC 2047) ; None si vide
    return result


def _extract_message_ids(value: Optional[str]) -> list[str]:
    """Extract all <message-id> tokens from a header value."""
    if not value:       # en-tête absent : retourne une liste vide
        return []
    return _MESSAGE_ID_RE.findall(value)  # retourne tous les contenus entre < > trouvés dans la chaîne


def _clean_message_id(value: Optional[str]) -> Optional[str]:
    """Normalise a Message-ID header to the bare id without angle brackets."""
    if not value:       # valeur absente ou vide : pas de message-ID utilisable
        return None
    # Try to extract from angle brackets first.
    found = _MESSAGE_ID_RE.findall(value)  # cherche la forme standard <id@domaine>
    if found:
        return found[0].strip()            # retourne le premier ID trouvé, sans chevrons ni espaces
    # Fallback: strip whitespace and angle brackets directly.
    return value.strip().strip("<>").strip() or None  # nettoyage manuel si les chevrons sont mal formés ; None si résultat vide


# ---------------------------------------------------------------------------
# MIME part helpers
# ---------------------------------------------------------------------------


def _has_calendar_part(msg: email.message.Message) -> bool:
    """Return True if any MIME part has content-type text/calendar."""
    for part in msg.walk():              # parcourt récursivement toutes les parties MIME du message
        ct = part.get_content_type()     # retourne le type MIME de la partie (ex: "text/plain")
        if ct == "text/calendar":        # invitation de calendrier iCal : ce message doit être ignoré
            return True
    return False  # aucune partie calendar trouvée


def _decode_payload(part: email.message.Message) -> Optional[str]:
    """Decode a text MIME part payload to a string."""
    charset = part.get_content_charset() or "utf-8"  # charset déclaré dans Content-Type, utf-8 par défaut
    try:
        raw = part.get_payload(decode=True)         # décode le transfer-encoding (base64, quoted-printable…) → bytes bruts
        if not isinstance(raw, bytes):              # get_payload peut retourner str ou list pour les multipart : on rejette
            return None
        return raw.decode(charset, errors="replace")  # décode les bytes en str avec le charset déclaré ; remplace les octets invalides
    except Exception:  # charset inconnu, payload corrompu : retourne None plutôt que de planter
        return None


def _is_attachment_part(part: email.message.Message) -> bool:
    """Return True if this MIME part is an attachment (not inline text/html)."""
    disposition = part.get_content_disposition()           # lit Content-Disposition : "attachment", "inline" ou None
    if disposition and disposition.lower() == "attachment":  # Content-Disposition: attachment → c'est une pièce jointe explicite
        return True
    ct = part.get_content_maintype()                       # type principal : "text", "image", "application", etc.
    # Inline text/plain and text/html are body parts, not attachments.
    return ct not in ("text", "multipart")  # tout ce qui n'est pas texte ou conteneur multipart est traité comme pièce jointe


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse(msg: email.message.Message) -> ParsedMessage:
    """Parse *msg* into a ParsedMessage.

    Raises SkipMessage when the message should not be imported:
      - Any MIME part has content-type text/calendar.
      - Message has no Message-ID header (cannot deduplicate).
      - Message has no usable body (text, html, or attachments).
    """
    # --- Skip: calendar invites ---
    if _has_calendar_part(msg):                               # vérifie la présence d'une partie text/calendar
        raise SkipMessage("contains text/calendar part")      # abandon : invitation de calendrier

    # --- Skip: no Message-ID (cannot deduplicate) ---
    raw_mid = msg.get("Message-ID")                           # lit l'en-tête Message-ID brut (peut contenir des espaces)
    message_id_header = _clean_message_id(raw_mid)            # normalise : supprime les chevrons et espaces superflus
    if not message_id_header:                                 # absence de Message-ID : impossible de déduplicater en base
        raise SkipMessage("missing Message-ID header")        # abandon : message non identifiable

    # --- Headers ---
    in_reply_to_raw = msg.get("In-Reply-To")                  # en-tête brut de la réponse directe (peut contenir plusieurs IDs)
    in_reply_to_ids = _extract_message_ids(in_reply_to_raw)   # extrait les message-IDs entre chevrons
    in_reply_to = in_reply_to_ids[0] if in_reply_to_ids else None  # on ne garde que le premier ID (standard RFC 2822)

    references_header = _extract_message_ids(msg.get("References", ""))  # extrait tous les IDs de la chaîne de références

    subject = _decode_header(msg.get("Subject"))   # décode l'objet (potentiellement encodé RFC 2047)

    sent_at = _parse_date(msg.get("Date"))         # parse la date d'envoi et la normalise en UTC

    raw_from = msg.get("From", "")                 # en-tête From brut, chaîne vide si absent
    from_pairs = _parse_addresses(raw_from)        # parse le From en liste de (nom, adresse)
    if from_pairs:                                 # au moins une adresse trouvée
        from_name, from_address = from_pairs[0]   # on prend la première adresse de l'expéditeur
    else:                                          # From malformé ou absent : on tente de récupérer quelque chose
        from_name = None                           # pas de nom d'affichage
        from_address = _decode_header(raw_from) or ""  # utilise la valeur brute décodée, ou chaîne vide

    # Collect all headers as a plain dict for raw_headers JSONB column.
    # Duplicate header names are joined with a newline.
    raw_headers: dict[str, str] = {}               # dictionnaire de tous les en-têtes pour stockage en JSONB
    for key, val in msg.items():                   # itère sur toutes les paires (nom, valeur) du message
        decoded_val = _decode_header(val) or ""    # décode chaque valeur d'en-tête depuis RFC 2047
        if key in raw_headers:                     # en-tête dupliqué (ex: Received apparaît plusieurs fois)
            raw_headers[key] = raw_headers[key] + "\n" + decoded_val  # concatène avec un saut de ligne
        else:
            raw_headers[key] = decoded_val         # premier occurrence : ajout direct

    # --- MIME walk: extract body and attachments ---
    body_text: Optional[str] = None    # corps texte brut, initialisé à None ; on prend la première partie text/plain
    body_html: Optional[str] = None    # corps HTML, initialisé à None ; on prend la première partie text/html
    attachments: list[Attachment] = [] # liste des pièces jointes trouvées
    recipients: list[Recipient] = []   # liste des destinataires (remplie plus bas)

    for part in msg.walk():                        # parcourt récursivement toutes les parties MIME
        ct = part.get_content_type()               # type MIME de la partie courante

        # Always skip calendar parts (belt-and-suspenders; already checked above).
        if ct == "text/calendar":                  # sécurité supplémentaire : au cas où walk() trouverait une partie calendar imbriquée
            continue

        # Skip multipart containers — their children are visited by walk().
        if part.get_content_maintype() == "multipart":  # les conteneurs multipart/* ne contiennent pas de payload direct
            continue

        if ct == "text/plain" and not _is_attachment_part(part) and body_text is None:  # première partie texte inline
            body_text = _decode_payload(part)      # décode et stocke le corps texte brut

        elif ct == "text/html" and not _is_attachment_part(part) and body_html is None:  # première partie HTML inline
            body_html = _decode_payload(part)      # décode et stocke le corps HTML

        elif _is_attachment_part(part):            # tout le reste (image, pdf, texte en attachement…)
            raw_data = part.get_payload(decode=True)      # décode le transfer-encoding → bytes
            if not isinstance(raw_data, bytes):           # payload vide ou structure inattendue : on ignore cette partie
                continue
            filename = _decode_header(part.get_filename())  # nom du fichier depuis Content-Disposition ou Content-Type name=
            content_id_raw = part.get("Content-ID")         # Content-ID pour les images inline référencées par cid: dans le HTML
            content_id = _clean_message_id(content_id_raw)  # normalise le Content-ID (même format que Message-ID)
            attachments.append(                    # ajoute la pièce jointe à la liste
                Attachment(
                    content_type=ct,               # type MIME exact (ex: "image/png")
                    filename=filename,             # nom décodé, ou None si absent
                    content_id=content_id,         # ID pour les ressources inline, ou None
                    size_bytes=len(raw_data),      # taille réelle en octets après décodage
                    data=raw_data,                 # contenu binaire brut à stocker en BYTEA
                )
            )

    # --- Skip: empty message ---
    has_content = bool(body_text or body_html or attachments)  # True si au moins un contenu utilisable existe
    if not has_content:                                        # message sans corps ni pièce jointe : rien à stocker
        raise SkipMessage("no usable content (no body text, html, or attachments)")

    # --- Recipients ---
    for header_name, rtype in (("To", "to"), ("Cc", "cc"), ("Bcc", "bcc")):  # parcourt les 3 types de destinataires
        for display_name, address in _parse_addresses(msg.get(header_name)):  # parse chaque en-tête de destinataires
            recipients.append(
                Recipient(recipient_type=rtype, address=address, display_name=display_name)  # crée un objet Recipient par adresse
            )

    return ParsedMessage(                          # construit et retourne le message analysé, prêt pour l'insertion en base
        message_id_header=message_id_header,       # clé de déduplication
        in_reply_to=in_reply_to,                   # parent direct dans le fil
        references_header=references_header,       # chaîne complète des ancêtres
        subject=subject,                           # objet décodé
        sent_at=sent_at,                           # date UTC
        from_address=from_address,                 # adresse expéditeur
        from_name=from_name,                       # nom expéditeur
        body_text=body_text,                       # corps texte
        body_html=body_html,                       # corps HTML
        raw_headers=raw_headers,                   # tous les en-têtes pour JSONB
        recipients=recipients,                     # liste To/Cc/Bcc
        attachments=attachments,                   # liste des pièces jointes
    )
