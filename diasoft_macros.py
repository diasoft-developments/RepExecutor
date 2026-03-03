import re
from utils_logger import log_execution

@log_execution()
def parse_diasoft_macros(content: str) -> str:
    content = replace_nolock_index(content)
    content = replace_rowlock_index(content)
    content = replace_updlock_index(content)
    
    
    content = replace_forceplan(content)
    # content = replace_forceplan(content)
    content = replace_isolat(content)
    content = replace_forceolan_off(content)
    content = suser_name(content)
    
    return content

@log_execution()
def suser_name(content: str) -> str:
    """
    Заменяет #M_FORCEPLAN на set rowcount 0
    """
    return re.sub(
        r"#SUSER_NAME\b",
        "suser_name()",
        content,
        flags=re.IGNORECASE
    )

@log_execution()
def replace_nolock_index(content: str) -> str:
    """
    Заменяет конструкции вида:
    #M_NOLOCK_INDEX( XPKtPropertyUs )
    #M_NOLOCK_INDEX (  XPKtPropertyUs )
    #M_NOLOCK_INDEX (  XPKtPropertyUs)

    на:
    with ( nolock index=XPKtPropertyUs )
    """
    pattern = re.compile(
        r"#M_NOLOCK_INDEX\s*\(\s*([A-Za-z0-9_]+)\s*\)",
        flags=re.IGNORECASE
    )

    return pattern.sub(r"with (nolock index=\1)", content)

@log_execution()
def replace_rowlock_index(content: str) -> str:
    """
    Заменяет конструкции вида:
    #M_ROWLOCK_INDEX( XPKtPropertyUs )
    #M_ROWLOCK_INDEX (  XPKtPropertyUs )
    #M_ROWLOCK_INDEX (  XPKtPropertyUs)

    на:
    with ( rowlock index=XPKtPropertyUs )
    """
    pattern = re.compile(
        r"#M_ROWLOCK_INDEX\s*\(\s*([A-Za-z0-9_]+)\s*\)",
        flags=re.IGNORECASE
    )

    return pattern.sub(r"with (rowlock index=\1)", content)

@log_execution()
def replace_updlock_index(content: str) -> str:
    """
    Заменяет конструкции вида:
    #M_UPDLOCK_INDEX ( XPKtPropertyUs )
    #M_UPDLOCK_INDEX (  XPKtPropertyUs )
    #M_UPDLOCK_INDEX (  XPKtPropertyUs)

    на:
    with ( updlock index=XPKtPropertyUs )
    """
    pattern = re.compile(
        r"#M_UPDLOCK_INDEX\s*\(\s*([A-Za-z0-9_]+)\s*\)",
        flags=re.IGNORECASE
    )

    return pattern.sub(r"with (updlock index=\1)", content)

@log_execution()
def replace_forceplan(content: str) -> str:
    """
    Заменяет #M_FORCEPLAN на set rowcount 0
    """
    return re.sub(
        r"#M_FORCEPLAN\b",
        "set rowcount 0;",
        content,
        flags=re.IGNORECASE
    )

@log_execution()
def replace_isolat(content: str) -> str:
    """
    Удаляет #M_ISOLAT (в любом регистре)
    """
    return re.sub(r"#M_ISOLAT\b", "", content, flags=re.IGNORECASE)    

@log_execution()
def replace_forceolan_off(content: str) -> str:
    """
    Удаляет #M_FORCEPLAN_OFF (в любом регистре)
    """
    return re.sub(r"#M_FORCEPLAN_OFF\b", "", content, flags=re.IGNORECASE)    