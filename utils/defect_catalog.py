"""Справочник типов дефектов.

Единый источник маппинга ключей дефектов на русские названия.
Используется в:
    - defect_extractor.py — для генерации промпта
    - excel_generator.py — для отображения русских названий
"""

from __future__ import annotations


# =============================================================================
# Справочник дефектов: ключ → русское название
# =============================================================================

DEFECT_CATALOG: dict[str, str] = {
    # Вентиляция (ventilation_*)
    "ventilation_system_malfunction": "Работоспособность системы",
    "ventilation_project_mismatch": "Соответствие проекту",
    "ventilation_wall_ceiling_gap": "Зазор по стене/потолку",
    "ventilation_surface_defects": "Дефекты поверхности",

    # Отопление (heating_*)
    "heating_pipes_joint_overlap": "Перекрытие швов",
    "heating_pipes_surface_defects": "Дефекты поверхности",
    "heating_pipes_sewerage": "Канализация",
    "heating_pipes_gaps": "Зазоры",
    "heating_pipes_fire_protection": "Противопожарный водопровод и спринклерное пожаротушение",
    "heating_pipes_water_supply": "Водопровод",
    "heating_pipes_cold_supply": "Холодоснабжение",
    "heating_pipes_paint_defects": "Дефекты окраски труб отопления",

    # Обои (wallpaper_*)
    "wallpaper_paint_uniformity": "Равномерность окраски",
    "wallpaper_surface_chalking": "Меление поверхности",
    "wallpaper_surface_defects": "Дефекты поверхности",
    "wallpaper_joints": "Стыки",
    "wallpaper_peeling": "Отслоения",
    "wallpaper_gluing_surface_defects": "Дефекты поверхности",
    "wallpaper_glue_stains": "Загрязнения, следы клея на поверхности",
    "wallpaper_overlap": "Нахлест",

    # Окна (window_*)
    "window_mounting_seam_mismatch": "Монтажный шов не соответствует проекту",
    "window_trim_cracks_gaps": "Трещины, зазоры в примыкании пластиковых нащельников к откосам",
    "window_adjustment_missing": "Не выполнена регулировка",
    "window_glazing_beads_missing": "Отсутствие, повреждение штапиков",
    "window_trim_incorrect_mounting": "Некорректный монтаж нащельников",
    "window_hardware_missing": "Отсутствие, повреждение фурнитуры",
    "window_slopes_paint_uniformity": "Равномерность окраски откосов",
    "window_slopes_surface_defects": "Дефекты поверхности откосов",

    # Входная дверь (entrance_*)
    "entrance_door_reinstall_needed": "Демонтаж, монтаж двери",
    "entrance_door_adjustment_missing": "Не выполнена регулировка",
    "entrance_door_trim_missing": "Отсутствие примыкания доборов и наличников",
    "entrance_door_hardware_damage": "Мех. повреждения фурнитуры и др.",
    "entrance_door_cleanliness": "Чистота",
    "entrance_door_surface_defects": "Дефекты поверхности",
    "entrance_door_opening_filling": "Заполнение проемов",
    "entrance_door_locking_devices": "Запирающие устройства",

    # Межкомнатные двери (interior_*)
    "interior_door_adjustment_missing": "Не выполнена регулировка дверного блока",
    "interior_door_surface_defects": "Дефекты поверхности",
    "interior_door_hardware_adjustment": "Не выполнена регулировка фурнитуры",

    # Доборы и наличники (door_*)
    "door_trim_connection_gaps": "Зазор в соединениях",
    "door_trim_mounting": "Крепление",
    "door_trim_wall_gaps": "Зазор по стенам",
    "door_trim_surface_defects": "Дефекты поверхности",

    # Балкон/лоджия (balcony_*)
    "balcony_tile_steps_chips": "Плитка пол — уступы, сколы",
    "balcony_paint_drips_stains": "Пропуски, потеки, окрашивания стен и потолков",
    "balcony_tile_grout_issues": "Плитка пол — пропуски, излишки затирки",

    # Плинтусы (baseboards_*)
    "baseboards_surface_defects": "Дефекты поверхности",
    "baseboards_floor_gaps": "Зазоры полы",
    "baseboards_connecting_elements": "Соединительные элементы",
    "baseboards_joint_overlap": "Перекрытие швов",
    "baseboards_insufficient_fasteners": "Недостаточное количество крепежей",

    # Пороги (threshold_*)
    "threshold_steps": "Уступы",

    # Потолок покраска (ceiling_*)
    "ceiling_paint_uniformity": "Равномерность окраски",
    "ceiling_surface_defects": "Дефекты поверхности",

    # Натяжной потолок (stretch_*)
    "stretch_ceiling_embedded_parts": "Выпирание закладных деталей",
    "stretch_ceiling_contamination": "Загрязнение полотна",
    "stretch_ceiling_baseboard_gap": "Зазор между стеной и потолочным плинтусом",
    "stretch_ceiling_pipe_gap": "Зазор у труб стояков отопления",
    "stretch_ceiling_sagging": "Втягивание полотна потолка",

    # Ревизионный люк (inspection_*)
    "inspection_hatch_door_adjustment": "Регулировка дверцы люка",
    "inspection_hatch_vertical_deviation": "Отклонение от вертикали",
    "inspection_hatch_surface_defects": "Дефекты поверхности",
    "inspection_hatch_wall_gap": "Зазор на стене",

    # Плитка пол (floor_*)
    "floor_tile_voids": "Пустоты",
    "floor_tile_layout_mismatch": "Раскладка не соответствует проекту",
    "floor_tile_grout": "Затирка",
    "floor_tile_unevenness": "Неровности по плоскости более 4 мм на 2 м рейку",
    "floor_tile_joint_displacement": "Смещение швов",
    "floor_tile_cracks_chips": "Трещины и сколы",
    "floor_tile_joint_placement": "Расположение швов",
    "floor_tile_steps": "Уступы",
    "floor_tile_joint_width": "Ширина швов",
    "floor_level_deviation": "Отклонение уровня пола более 4 мм на 2 м",

    # Плитка стены (wall_*)
    "wall_tile_joint_displacement": "Смещение швов",
    "wall_tile_glue_residue": "Остатки клея",
    "wall_tile_layout_mismatch": "Раскладка не соответствует проекту",
    "wall_tile_unevenness": "Неровности по плоскости более 2 мм",
    "wall_tile_grout": "Затирка",
    "wall_tile_steps": "Уступы более 1 мм",
    "wall_tile_voids": "Пустоты",
    "wall_tile_hole_shapes": "Формы отверстий",
    "wall_tile_cracks_chips": "Трещины и сколы",
    "wall_tile_joint_width": "Ширина швов",

    # Сантехника (plumbing_*)
    "plumbing_leaks_malfunction": "Протечки и неисправность",
    "plumbing_joint_sealing": "Герметизация швов",
    "plumbing_surface_defects": "Дефекты поверхности",
    "plumbing_mounting": "Крепление",
    "plumbing_mechanical_damage": "Механические повреждения",
    "plumbing_decorative_covers": "Декоративные накладки",

    # Ламинат (laminate_*)
    "laminate_chips_scratches": "Сколы, царапины, разнотон досок ламината",
    "laminate_board_gaps": "Зазоры между досками ламината",
    "laminate_ruler_gap": "Зазор между 2х метровой рейкой более 2мм",
    "laminate_steps": "Уступы",
    "laminate_floor_level_deviation": "Отклонение уровня пола более 4 мм на 2 м рейку",
    "laminate_wall_gap_missing": "Отсутствует или менее 10 мм зазор между ламинатом и вертикальными конструкциями",

    # Прочее
    "bath_screen_not_fixed": "Не закреплен экран под ванну",
    "wet_cleaning": "Влажная уборка",
}


# =============================================================================
# Вспомогательные функции
# =============================================================================


def get_defect_name_ru(key: str) -> str:
    """Возвращает русское название дефекта по ключу.

    Если ключ не найден в справочнике, возвращает сам ключ.

    Args:
        key: Ключ дефекта (например, "stretch_ceiling_baseboard_gap")

    Returns:
        Русское название (например, "Зазор между стеной и потолочным плинтусом")
    """
    return DEFECT_CATALOG.get(key, key)


def get_defect_reference_for_prompt() -> str:
    """Генерирует список дефектов для промпта LLM.

    Формат: "- key: Русское название"

    Returns:
        Строка со списком дефектов для вставки в промпт
    """
    lines = []
    for key, name_ru in DEFECT_CATALOG.items():
        lines.append(f"- {key}: {name_ru}")
    return "\n".join(lines)
