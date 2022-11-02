create table fund_port_interval_style(
    id                      bigint(20)         NOT NULL         AUTO_INCREMENT  comment 'ID',
    fund                    int(11)            NOT NULL         comment '内部编码',
    end_date                datetime           NOT NULL         comment '交易日/持仓公布日',
    code                    varchar(20)        NOT NULL         comment '资产代码',
    style_type              varchar(20)        NOT NULL         comment '风格类型',
    style                   varchar(20)        NOT NULL         comment '风格名称',
    weight                  decimal(18, 6)     NOT NULL         comment '风格权重',
    update_time             timestamp          NOT NULL         comment '更新时间'  DEFAULT  CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    jsid                    bigint(20)         NOT NULL,
    UNIQUE KEY  jsid_ (jsid),
    UNIQUE KEY  fund_code_date_type_name (fund, end_date, code, style_type, style),
    KEY fund_ (fund),
    KEY fund_code (fund, code),
    KEY fund_code_type (fund, code, style_type),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `zhijunfund_analysis`.`t_sys_seq`(`name`, `value`, `begin`, `step`, `version`, `update_time`, `remark`) VALUES ('fund_style_allocation', 1, 1, 1000, 0, now(), NULL);


create table client_port_interval_style(
    id                      bigint(20)         NOT NULL         AUTO_INCREMENT  comment 'ID',
    client_id               int(11)            NOT NULL         comment '内部编码',
    end_date                datetime           NOT NULL         comment '交易日/持仓公布日',
    code                    varchar(20)        NOT NULL         comment '资产代码',
    style_type              varchar(20)        NOT NULL         comment '风格类型',
    style                   varchar(20)        NOT NULL         comment '风格名称',
    weight                  decimal(18, 6)     NOT NULL         comment '风格权重',
    update_time             timestamp          NOT NULL         comment '更新时间'  DEFAULT  CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    jsid                    bigint(20)         NOT NULL,
    UNIQUE KEY  jsid_ (jsid),
    UNIQUE KEY  fund_code_date_type_name (client_id, end_date, code, style_type, style),
    KEY fund_ (client_id),
    KEY fund_code (client_id, code),
    KEY fund_code_type (client_id, code, style_type),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `zhijunfund_analysis`.`t_sys_seq`(`name`, `value`, `begin`, `step`, `version`, `update_time`, `remark`) VALUES ('fund_style_allocation', 1, 1, 1000, 0, now(), NULL);
