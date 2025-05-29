-- Обновление таблицы operator_wild_responsibility для добавления новых полей

-- Добавление поля senior_operator (старший оператор, null по умолчанию)
ALTER TABLE public.operator_wild_responsibility ADD COLUMN IF NOT EXISTS senior_operator varchar(100) DEFAULT NULL;

-- Добавление поля session_id (идентификатор сессии, null по умолчанию)
ALTER TABLE public.operator_wild_responsibility ADD COLUMN IF NOT EXISTS session_id varchar(100) DEFAULT NULL;

-- Полное определение таблицы после обновления
/*
CREATE TABLE public.operator_wild_responsibility (
	id serial4 NOT NULL,
	operator_name varchar(100) NOT NULL,
	wild_code varchar(100) NOT NULL,
	order_count int4 NOT NULL,
	processing_time numeric(10, 2) NOT NULL,
	product_name varchar(255) NOT NULL,
	additional_data jsonb NULL,
	created_at timestamptz DEFAULT now() NULL,
	senior_operator varchar(100) DEFAULT NULL,
	session_id varchar(100) DEFAULT NULL,
	CONSTRAINT operator_wild_responsibility_pkey PRIMARY KEY (id)
);
*/
