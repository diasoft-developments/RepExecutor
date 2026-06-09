"""Тесты время-масок @t согласно спецификации Diasoft."""
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
    # Входное значение: "16:23:56" (час=16, минута=23, секунда=56)
    test_cases = [
        ("16:23:56", "@t1", "16:23", "Тип 1: hh:mm"),
        ("16:23:56", "@t2", "1623", "Тип 2: hhmm"),
        ("16:23:56", "@t3", "04:23pm", "Тип 3: hh:mm[am/pm] (16→4pm)"),
        ("16:23:56", "@t4", "16:23:56", "Тип 4: hh:mm:ss"),
        ("2026-06-02 16:23:56", "@t4", "16:23:56", "Тип 4: hh:mm:ss"),
        
        # Дополнительные тесты для am/pm
        ("04:23:10", "@t3", "04:23am", "Тип 3: 04:23 → 04:23am"),
        ("12:00:00", "@t3", "12:00pm", "Тип 3: 12:00 → 12:00pm (полдень)"),
        ("00:00:00", "@t3", "12:00am", "Тип 3: 00:00 → 12:00am (полуночь)"),
        ("00:30:00", "@t3", "12:30am", "Тип 3: 00:30 → 12:30am"),
        ("11:59:00", "@t3", "11:59am", "Тип 3: 11:59 → 11:59am"),
        ("13:00:00", "@t3", "01:00pm", "Тип 3: 13:00 → 01:00pm"),
        
        # Тесты с другими входными форматами
        ("2026-06-02 16:23", "@t1", "16:23", "Тип 1: вход hh:mm"),
        ("2026-06-02 14:13:17.903", "@t4", "14:13:17", "Тип 4: вход hh:mm → hh:mm:ss (секунды=00)"),
        
        # Пустое значение
        ("", "@t1", "", "Пустое значение"),
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