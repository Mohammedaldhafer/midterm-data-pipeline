import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional


# ============================================================
# Field Aliases
# ============================================================

FIELD_ALIASES = {
    "order_id": [
        "id_order",
        "order_id",
        "orderid",
    ],

    "customer_id": [
        "id_customer",
        "customer_id",
        "customerid",
    ],

    "email": [
        "customer_email",
        "email",
        "email_address",
    ],

    "phone": [
        "customer_phone",
        "phone",
        "phone_number",
    ],

    "date": [
        "order_date",
        "date",
        "created_at",
        "at_order",
    ],

    "price": [
        "price",
        "unit_price",
        "item_price",
    ],

    "total": [
        "total",
        "order_total",
        "total_amount",
        "amount",
    ],

    "status": [
        "status",
        "order_status",
        "payment_status",
    ],
}


# ============================================================
# Helper Functions
# ============================================================

def normalize_field_name(name: str) -> str:
    """
    Normalize a field name for comparison.
    """

    return (
        str(name)
        .strip()
        .lower()
        .replace("-", "_")
        .replace(" ", "_")
    )


def find_field(
    record: Dict[str, Any],
    logical_name: str
) -> Optional[str]:
    """
    Find the actual record field corresponding
    to a logical field name.
    """

    normalized = {
        normalize_field_name(key): key
        for key in record.keys()
    }

    aliases = FIELD_ALIASES.get(
        logical_name,
        []
    )

    for alias in aliases:

        if alias in normalized:
            return normalized[alias]

    return None


def clean_text(value: Any) -> str:
    """
    Convert a value to stripped text.
    """

    if value is None:
        return ""

    return str(value).strip()


def add_correction(
    corrections: List[Dict[str, Any]],
    field: str,
    original_value: Any,
    corrected_value: Any,
    rule_code: str
):
    """
    Add one correction to the audit trail.
    """

    corrections.append(
        {
            "field": field,
            "original_value": original_value,
            "corrected_value": corrected_value,
            "rule_code": rule_code,
        }
    )


# ============================================================
# Rule 1
# Arabic-Indic Digits -> Latin Digits
# ============================================================

ARABIC_DIGIT_MAP = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹",
    "01234567890123456789"
)


def normalize_arabic_digits(
    value: Any
) -> Optional[str]:

    if value is None:
        return None

    original = str(value)

    corrected = original.translate(
        ARABIC_DIGIT_MAP
    )

    if corrected != original:
        return corrected

    return None


# ============================================================
# Rule 2
# Remove Currency Text
# ============================================================

CURRENCY_PATTERNS = [
    r"ريال\s*يمني",
    r"ريال",
    r"ر\.ي",
    r"YER",
    r"yer",
]


def normalize_currency(
    value: Any
) -> Optional[str]:

    if value is None:
        return None

    original = str(value).strip()

    corrected = original

    for pattern in CURRENCY_PATTERNS:

        corrected = re.sub(
            pattern,
            "",
            corrected,
            flags=re.IGNORECASE
        )

    corrected = corrected.strip()

    if corrected != original:
        return corrected

    return None


# ============================================================
# Rule 3
# Remove Thousands Separators
# ============================================================

def normalize_thousands_separator(
    value: Any
) -> Optional[str]:

    if value is None:
        return None

    original = str(value).strip()

    # Only remove commas when the value looks numeric.
    candidate = original.replace(",", "")

    if re.fullmatch(
        r"[+-]?\d+(\.\d+)?",
        candidate
    ):

        if candidate != original:
            return candidate

    return None


# ============================================================
# Rule 4
# Normalize Phone Number
# ============================================================

def normalize_phone(
    value: Any
) -> Optional[str]:

    if value is None:
        return None

    original = str(value).strip()

    # Remove spaces, hyphens and parentheses.
    corrected = re.sub(
        r"[\s\-\(\)]",
        "",
        original
    )

    # Example:
    # 967+ 77 123 4567
    # becomes:
    # 967+771234567

    if corrected != original:
        return corrected

    return None


# ============================================================
# Rule 5
# Normalize Email Repeated Symbols
# ============================================================

def normalize_email(
    value: Any
) -> Optional[str]:

    if value is None:
        return None

    original = str(value).strip()

    corrected = original

    # Fix obvious repeated @ symbols.
    corrected = re.sub(
        r"@+",
        "@",
        corrected
    )

    # Fix obvious repeated dots.
    corrected = re.sub(
        r"\.{2,}",
        ".",
        corrected
    )

    # Remove spaces around email.
    corrected = corrected.replace(
        " ",
        ""
    )

    # Only accept if the resulting email
    # has exactly one @ and a domain dot.
    if (
        corrected != original
        and corrected.count("@") == 1
        and "." in corrected.split("@")[1]
    ):
        return corrected

    return None


# ============================================================
# Rule 6
# Normalize Date
# ============================================================

DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
]


def normalize_date(
    value: Any
) -> Optional[str]:

    if value is None:
        return None

    original = str(value).strip()

    for date_format in DATE_FORMATS:

        try:

            parsed = datetime.strptime(
                original,
                date_format
            )

            corrected = parsed.strftime(
                "%Y-%m-%d"
            )

            if corrected != original:
                return corrected

            return None

        except ValueError:
            continue

    return None


# ============================================================
# Rule 7
# Normalize Status / Synonyms
# ============================================================

STATUS_MAP = {
    "paid": "paid",
    "مدفوع": "paid",
    "دفع": "paid",

    "pending": "pending",
    "معلق": "pending",
    "قيد الانتظار": "pending",

    "cancelled": "cancelled",
    "canceled": "cancelled",
    "ملغي": "cancelled",

    "confirmed": "confirmed",
    "مؤكد": "confirmed",

    "completed": "completed",
    "مكتمل": "completed",
}


def normalize_status(
    value: Any
) -> Optional[str]:

    if value is None:
        return None

    original = str(value).strip()

    key = original.lower()

    if key in STATUS_MAP:

        corrected = STATUS_MAP[key]

        if corrected != original:
            return corrected

    return None


# ============================================================
# Rule 8
# Normalize Whitespace
# ============================================================

def normalize_whitespace(
    value: Any
) -> Optional[str]:

    if value is None:
        return None

    original = str(value)

    corrected = re.sub(
        r"\s+",
        " ",
        original
    ).strip()

    if corrected != original:
        return corrected

    return None


# ============================================================
# Rule 9
# Convert Known Number Words
# ============================================================

KNOWN_NUMBER_WORDS = {
    "ألف": "1000",
    "الف": "1000",
    "ألفان": "2000",
    "الفان": "2000",
    "ألفين": "2000",
    "الفين": "2000",
    "خمسة آلاف": "5000",
    "خمسه آلاف": "5000",
    "خمسة الاف": "5000",
}


def normalize_known_number_words(
    value: Any
) -> Optional[str]:

    if value is None:
        return None

    original = str(value).strip()

    key = original.lower()

    if key in KNOWN_NUMBER_WORDS:

        return KNOWN_NUMBER_WORDS[key]

    return None


# ============================================================
# Rule 10
# Normalize Numeric Values
# ============================================================

def normalize_numeric_value(
    value: Any
) -> Optional[str]:

    if value is None:
        return None

    original = str(value).strip()

    # Remove common currency text.
    cleaned = re.sub(
        r"(ريال\s*يمني|ريال|ر\.ي|YER)",
        "",
        original,
        flags=re.IGNORECASE
    )

    # Arabic digits.
    cleaned = cleaned.translate(
        ARABIC_DIGIT_MAP
    )

    # Thousands separators.
    cleaned = cleaned.replace(
        ",",
        ""
    )

    cleaned = cleaned.strip()

    try:

        number = Decimal(cleaned)

        # Keep integer formatting clean.
        if number == number.to_integral():

            corrected = str(
                int(number)
            )

        else:

            corrected = format(
                number,
                "f"
            )

        if corrected != original:
            return corrected

    except (
        InvalidOperation,
        ValueError
    ):
        pass

    return None


# ============================================================
# Apply One Field Rule
# ============================================================

def apply_rule(
    record: Dict[str, Any],
    field: str,
    rule_function,
    rule_code: str
) -> bool:

    if field not in record:
        return False

    original = record.get(field)

    corrected = rule_function(
        original
    )

    if corrected is None:
        return False

    record[field] = corrected

    return True


# ============================================================
# Quality Transformation
# ============================================================

def clean_record(
    record: Dict[str, Any]
) -> Dict[str, Any]:

    cleaned_record = dict(record)

    corrections = []

    # --------------------------------------------------------
    # Rule 1: Arabic digits
    # --------------------------------------------------------

    for field, value in list(
        cleaned_record.items()
    ):

        corrected = normalize_arabic_digits(
            value
        )

        if corrected is not None:

            add_correction(
                corrections,
                field,
                value,
                corrected,
                "ARABIC_DIGITS"
            )

            cleaned_record[field] = corrected

    # --------------------------------------------------------
    # Rule 2: Currency
    # --------------------------------------------------------

    price_field = find_field(
        cleaned_record,
        "price"
    )

    if price_field:

        original = cleaned_record[
            price_field
        ]

        corrected = normalize_currency(
            original
        )

        if corrected is not None:

            add_correction(
                corrections,
                price_field,
                original,
                corrected,
                "CURRENCY_NORMALIZATION"
            )

            cleaned_record[
                price_field
            ] = corrected

    # --------------------------------------------------------
    # Rule 3: Thousands separators
    # --------------------------------------------------------

    if price_field:

        original = cleaned_record[
            price_field
        ]

        corrected = normalize_thousands_separator(
            original
        )

        if corrected is not None:

            add_correction(
                corrections,
                price_field,
                original,
                corrected,
                "THOUSANDS_SEPARATOR"
            )

            cleaned_record[
                price_field
            ] = corrected

    # --------------------------------------------------------
    # Rule 4: Phone
    # --------------------------------------------------------

    phone_field = find_field(
        cleaned_record,
        "phone"
    )

    if phone_field:

        original = cleaned_record[
            phone_field
        ]

        corrected = normalize_phone(
            original
        )

        if corrected is not None:

            add_correction(
                corrections,
                phone_field,
                original,
                corrected,
                "PHONE_NORMALIZATION"
            )

            cleaned_record[
                phone_field
            ] = corrected

    # --------------------------------------------------------
    # Rule 5: Email
    # --------------------------------------------------------

    email_field = find_field(
        cleaned_record,
        "email"
    )

    if email_field:

        original = cleaned_record[
            email_field
        ]

        corrected = normalize_email(
            original
        )

        if corrected is not None:

            add_correction(
                corrections,
                email_field,
                original,
                corrected,
                "EMAIL_REPEATED_SYMBOLS"
            )

            cleaned_record[
                email_field
            ] = corrected

    # --------------------------------------------------------
    # Rule 6: Date
    # --------------------------------------------------------

    date_field = find_field(
        cleaned_record,
        "date"
    )

    if date_field:

        original = cleaned_record[
            date_field
        ]

        corrected = normalize_date(
            original
        )

        if corrected is not None:

            add_correction(
                corrections,
                date_field,
                original,
                corrected,
                "DATE_STANDARDIZATION"
            )

            cleaned_record[
                date_field
            ] = corrected

    # --------------------------------------------------------
    # Rule 7: Status
    # --------------------------------------------------------

    status_field = find_field(
        cleaned_record,
        "status"
    )

    if status_field:

        original = cleaned_record[
            status_field
        ]

        corrected = normalize_status(
            original
        )

        if corrected is not None:

            add_correction(
                corrections,
                status_field,
                original,
                corrected,
                "STATUS_STANDARDIZATION"
            )

            cleaned_record[
                status_field
            ] = corrected

    # --------------------------------------------------------
    # Rule 8: Whitespace
    # --------------------------------------------------------

    for field, value in list(
        cleaned_record.items()
    ):

        # Do not modify audit metadata.
        if field in {
            "corrections",
            "record_raw",
        }:
            continue

        corrected = normalize_whitespace(
            value
        )

        if corrected is not None:

            add_correction(
                corrections,
                field,
                value,
                corrected,
                "WHITESPACE_NORMALIZATION"
            )

            cleaned_record[field] = corrected

    # --------------------------------------------------------
    # Rule 9: Known number words
    # --------------------------------------------------------

    if price_field:

        original = cleaned_record[
            price_field
        ]

        corrected = normalize_known_number_words(
            original
        )

        if corrected is not None:

            add_correction(
                corrections,
                price_field,
                original,
                corrected,
                "KNOWN_NUMBER_WORD"
            )

            cleaned_record[
                price_field
            ] = corrected

    # --------------------------------------------------------
    # Rule 10: Numeric normalization
    # --------------------------------------------------------

    if price_field:

        original = cleaned_record[
            price_field
        ]

        corrected = normalize_numeric_value(
            original
        )

        if corrected is not None:

            add_correction(
                corrections,
                price_field,
                original,
                corrected,
                "NUMERIC_NORMALIZATION"
            )

            cleaned_record[
                price_field
            ] = corrected

    # --------------------------------------------------------
    # Quality Status
    # --------------------------------------------------------

    if corrections:

        quality_status = "corrected"

    else:

        quality_status = "valid"

    cleaned_record[
        "quality_status"
    ] = quality_status

    cleaned_record[
        "corrections"
    ] = corrections

    return cleaned_record


# ============================================================
# Public Function
# ============================================================

def apply_quality_rules(
    record: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Public API for cleaning one raw record.
    """

    return clean_record(record)


# ============================================================
# Simple Manual Test
# ============================================================

if __name__ == "__main__":

    test_record = {
        "id_order": "1001",
        "id_customer": "55",
        "customer_email": "user@@mail..com",
        "customer_phone": "967+ 77 123 4567",
        "price": "٥٠٠٠ ريال",
        "order_date": "2025/01/31",
        "status": "مدفوع",
    }

    result = apply_quality_rules(
        test_record
    )

    print("=" * 70)
    print("QUALITY RULES TEST")
    print("=" * 70)

    print(
        "Original record:"
    )

    print(test_record)

    print()
    print(
        "Cleaned record:"
    )

    print(result)

    print()
    print(
        "Corrections:"
    )

    for correction in result[
        "corrections"
    ]:

        print(
            correction
        )

    print("=" * 70)