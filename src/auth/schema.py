from pydantic import BaseModel

class UserPermissions(BaseModel):
    """Схема пермишинов пользователя"""
    user_id: int
    edit_users: bool
    reception_of_goods: bool
    moving_goods_between_warehouses: bool
    movement_of_goods_between_warehouse_zones: bool
    creating_a_delivery: bool
    converting_sz_to_hanging: bool
    transfer_of_delivery_to_delivery: bool
    creation_of_a_reserve_fbo: bool
    sending_a_reserve_fbo: bool
    changing_product_characteristics: bool
    changing_the_product_name: bool
    adding_a_new_product: bool
    return_acceptance: bool
    ability_to_upload_excel_file_to_fines: bool
    viewing: bool
    download_excel_files: bool
    crm_viewing_settings: bool
    crm_viewing_warehouse: bool
    crm_viewing_task_of_store: bool
    crm_viewing_orders: bool
    crm_viewing_products: bool
    crm_viewing_promotions: bool
    crm_viewing_unit_economics: bool
    crm_viewing_crm_analytic: bool
    crm_change_price_and_discounts: bool
    crm_possibility_to_store_leftovers: bool
    crm_ability_to_add_and_remove_products_from_promotions: bool