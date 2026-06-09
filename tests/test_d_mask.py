"""Тесты дата-масок @d согласно спецификации Diasoft."""
import pytest
import sys
sys.path.insert(0, '.')

from parsers.wrd_field_formatter import _apply_single_mask


def test_mask(mask, value, expected, description=""):
    """Проверяет результат применения маски к значению."""
    result = _apply_single_mask(str(value), mask)
    passed = result == expected
    status = "✓" if passed else "✗"
    print(f"{status} {description}")
    print(f"   Значение: '{value}', Маска: '{mask}'")
    print(f"   Ожидаемо: '{expected}' (длина={len(expected)})")
    print(f"   Получено: '{result}' (длина={len(result)})")
    print()
    return passed


def main():
    passed = 0
    total = 0

    # --- Тесты по таблице ---
    # Формат: (значение, маска, ожидаемый_результат, описание)
    test_cases = [
        ("2026-06-02 00:00:00", "@d1", "06/02/26", "Тип 1: mm/dd/yy"),
        ("2026-06-09", "@d2", "06/09/2026", "Тип 2: mm/dd/yyyy"),
        ("2026-06-09", "@d3", "Июн 09, 2026", "Тип 3: mon dd, yyyy"),
        ("2026-06-09", "@d4", "09 Июня 2026", "Тип 4: dd Month yyyy"),
        ("2026-06-09", "@d5", "09/06/26", "Тип 5: dd/mm/yy"),
        ("2026-06-09", "@d6", "09/06/2026", "Тип 6: dd/mm/yyyy"),
        ("2026-06-09", "@d7", "09 Июн 26", "Тип 7: dd mon yy"),
        ("2026-06-09", "@d8", "09 Июн 2026", "Тип 8: dd mon yyyy"),
        ("2026-06-09", "@d9", "26/06/09", "Тип 9: yy/mm/dd"),
        ("2026-06-09", "@d10", "2026/06/09", "Тип 10: yyyy/mm/dd"),
        ("2026-06-09", "@d11", "260609", "Тип 11: yymmdd"),
        ("2026-06-09", "@d13", "Вто, 09 Июн 26", "Тип 13: wd, dd mon yy"),
        ("2026-06-09", "@d14", "Вторник, 09 Июня 2026", "Тип 14: weekday, dd Month yyyy"),
        ("2026-06-09", "@d1-", "06-09-26", "Тип 1 с разделителем -"),
        ("2026-06-09", "@d10-", "2026-06-09", "Тип 10 с разделителем -"),
        ("2026-06-09", "@d1.", "06.09.26", "Тип 1 с разделителем ."),
        ("None", "@d1b", "", "Флаг b: пусто при None"),
        ("2026-06-09", "@d10t", "20260609", "Флаг t: без разделителей"),
        ("2026-06-09", "@d10-t", "20260609", "Тип 10 с разделителем - и флагом t"),
        ("2026-06-09", "@db", "09 июня 2026 года", "Дата прописью: @db"),
        ("2026-06-09", "@do", "09-го июня 2026 года", "Порядковый формат: @do"),
        ("", "@d1", "", "Пустое значение")
    ]

    for value, mask, expected, description in test_cases:
        total += 1
        if test_mask(mask, value, expected, description):
            passed += 1

    print(f"\n{'='*60}")
    print(f"Результат: {passed}/{total} тестов пройдено")
    if passed == total:
        print("Все тесты пройдены успешно! ✓")
    else:
        print(f"Не пройдено: {total - passed} ✗")


if __name__ == '__main__':
    main()

