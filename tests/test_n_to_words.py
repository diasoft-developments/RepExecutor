"""Тесты для n_to_words.py"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parsers.n_to_words import num_to_russian_words, number_to_words, _get_number_word_form


def test_num_to_russian_words():
    """Тест преобразования чисел в пропись."""
    tests = [
        (0, 'ноль'),
        (1, 'один'),
        (2, 'два'),
        (5, 'пять'),
        (21, 'двадцать один'),
        (100, 'сто'),
        (1000, 'одна тысяча'),
        (2000, 'две тысячи'),
        (1000000, 'один миллион'),
        (1234567, 'один миллион двести тридцать четыре тысячи пятьсот шестьдесят семь'),
    ]
    
    print("=== Тест num_to_russian_words ===")
    for n, expected in tests:
        result = num_to_russian_words(n)
        status = "OK" if result == expected else "FAIL"
        print(f"  {n}: {result} (ожидается: {expected}) [{status}]")


def test_get_number_word_form():
    """Тест выбора формы слова."""
    forms = ["целая", "целой", "целых"]
    
    tests = [
        (1, "целая"),
        (21, "целая"),
        (101, "целая"),
        (2, "целой"),
        (3, "целой"),
        (4, "целой"),
        (22, "целой"),
        (5, "целых"),
        (10, "целых"),
        (11, "целых"),
        (15, "целых"),
        (100, "целых"),
        (150, "целых"),
    ]
    
    print("\n=== Тест _get_number_word_form ===")
    for num, expected in tests:
        result = _get_number_word_form(num, forms)
        status = "OK" if result == expected else "FAIL"
        print(f"  {num}: {result} (ожидается: {expected}) [{status}]")


def test_number_to_words():
    """Тест number_to_words с различными параметрами."""
    int_parts = ["целая", "целой", "целых"]
    frac_parts = ["копейка", "копейки", "копеек"]
    
    print("\n=== Тест number_to_words ===")
    
    # Базовый случай: 150 целых
    result = number_to_words(150, int_parts, no_zero_frac=True)
    print(f"  150: {result}")
    
    # С дробной частью: 150.50
    result = number_to_words(150.50, int_parts, frac_parts, frac_part_definition=False, no_zero_frac=True)
    print(f"  150.50 с FracParts: {result}")
    
    # С FracPartDefinition=True
    result = number_to_words(150.50, int_parts, frac_part_definition=True)
    print(f"  150.50 с FracPartDefinition: {result}")
    
    # IntOnly=True
    result = number_to_words(150.50, int_parts, int_only=True)
    print(f"  150.50 IntOnly: {result}")
    
    # LeadingZero=False, число 0.50
    result = number_to_words(0.50, int_parts, frac_part_definition=True, leading_zero=False)
    print(f"  0.50 LeadingZero=False: {result}")
    
    # FracPartDigital=True
    result = number_to_words(150.50, int_parts, frac_parts, frac_part_definition=False, frac_part_digital=True)
    print(f"  150.50 FracPartDigital: {result}")
    
    result = number_to_words(8.50, int_parts, frac_parts)
    print(f"  8.50: {result}")    


def test_frac_scale_detection():
    """Тест определения масштаба дробной части (десятые vs сотые)."""
    int_parts = ["целая", "целой", "целых"]
    
    print("\n=== Тест определения масштаба ===")
    
    # 0.1 — один знак → десятых
    result = number_to_words(0.1, int_parts, frac_part_definition=True, leading_zero=False)
    expected = "один десятых"
    status = "OK" if expected in result else "FAIL"
    print(f"  0.1: {result} (ожидается '{expected}' в результате) [{status}]")
    
    # 0.12 — два знака → сотых
    result = number_to_words(0.12, int_parts, frac_part_definition=True, leading_zero=False)
    expected = "двенадцать сотых"
    status = "OK" if expected in result else "FAIL"
    print(f"  0.12: {result} (ожидается '{expected}' в результате) [{status}]")
    
    # 1.5 — один знак → десятых → 5 десятых
    result = number_to_words(1.5, int_parts, frac_part_definition=True)
    expected = "пять десятых"
    status = "OK" if expected in result else "FAIL"
    print(f"  1.5: {result} (ожидается '{expected}' в результате) [{status}]")
    
    # 1.23 — два знака → сотых
    result = number_to_words(1.23, int_parts, frac_part_definition=True)
    expected = "двадцать три сотых"
    status = "OK" if expected in result else "FAIL"
    print(f"  1.23: {result} (ожидается '{expected}' в результате) [{status}]")
    
    # 0.10 — Python float не различает 0.10 и 0.1
    # Без digits=2 это будет один десятых (т.к. 0.10 == 0.1 в float)
    result = number_to_words(0.10, int_parts, frac_part_definition=True, leading_zero=False)
    expected = "один десятых"
    status = "OK" if expected in result else "FAIL"
    print(f"  0.10 (без digits): {result} (ожидается '{expected}' в результате) [{status}]")
    
    # 0.10 с digits=2 → явно указываем масштаб сотых
    result = number_to_words(0.10, int_parts, frac_part_definition=True, leading_zero=False, digits=2)
    expected = "десять сотых"
    status = "OK" if expected in result else "FAIL"
    print(f"  0.10 (digits=2): {result} (ожидается '{expected}' в результате) [{status}]")
 

if __name__ == '__main__':
    test_num_to_russian_words()
    test_get_number_word_form()
    test_number_to_words()
    test_frac_scale_detection()
