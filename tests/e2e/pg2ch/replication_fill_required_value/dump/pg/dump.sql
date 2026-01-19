CREATE EXTENSION hstore;

-- needs to be sure there is db1
create table customers_customerprofile
(
    id                          integer                   not null PRIMARY KEY,
    created_at                  timestamp with time zone  not null,
    last_active_at              timestamp with time zone  not null,
    redirect_to                 character varying(100)            ,
    variable_dict               jsonb                     not null,
    bot_id                      integer                   not null,
    primary_node_id             integer                           ,
    profile_id                  integer                   not null,
    history                     text[]                            ,
    uuid                        uuid                      not null,
    error_at                    timestamp with time zone          ,
    error_reason                text                              ,
    first_name                  character varying(100)            ,
    last_name                   character varying(100)            ,
    messenger_id                character varying(200)    not null,
    platform                    character varying(20)     not null,
    viber_api_version           smallint                  not null,
    viber_phone_number          character varying(20)             ,
    expected_inputs             hstore                            ,
    cached_qr_keyboard          bytea                             ,
    msgs_with_markup            integer[]                         ,
    tracking_data               hstore                            ,
    chat_center_last_active_at  timestamp with time zone          ,
    chat_center_mode            boolean                   not null,
    chat_center_request_status  character varying(12)             ,
    bot_last_active_at          timestamp with time zone          ,
    operator_last_active_at     timestamp with time zone          ,
    chat_center_session_id      integer                           ,
    last_interaction            jsonb                             ,
    avatar                      character varying(100)            ,
    avatar_updated_at           timestamp with time zone          ,
    god_mode                    boolean                   not null,
    status                      character varying(32)     not null,
    last_email_subject          character varying(250)            ,
    browser                     character varying(128)            ,
    current_page                text                              ,
    device                      character varying(128)            ,
    invite_page                 text                              ,
    ip_address                  character varying(128)            ,
    operation_system            character varying(64)             ,
    city                        character varying(128)            ,
    status_changed              timestamp with time zone  not null,
    last_active_type            character varying(12)             ,
    user_last_active_at         timestamp with time zone          ,
    username                    character varying(100)            ,
    usedesk_chat_id             bigint
);

insert into customers_customerprofile (id, created_at, last_active_at, variable_dict, bot_id, profile_id, uuid, messenger_id, platform, viber_api_version, chat_center_mode, god_mode, status, status_changed)
values (0, '2004-10-19 10:23:54+02', '2004-10-19 10:23:54+02', '{}', 0, 0, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'messenger_id', 'platform', 0, true, true, 'status', '2004-10-19 10:23:54+02')
;
