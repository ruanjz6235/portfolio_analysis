create table fund_style_allocation(
    id                      bigint(20)         NOT NULL         AUTO_INCREMENT  comment 'ID',
    fund                    int(11)            NOT NULL         comment '内部编码',
    end_date                datetime           NOT NULL         comment '交易日/持仓公布日',
    style_type              varchar(20)        NOT NULL         comment '风格类型',
    style                   varchar(20)        NOT NULL         comment '风格名称',
    weight                  decimal(18, 6)     NOT NULL         comment '风格权重',
    va                      decimal(18, 6)     NOT NULL         comment '风格归因',
    update_time             timestamp          NOT NULL         comment '更新时间'  DEFAULT  CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    jsid                    bigint(20)         NOT NULL,
    UNIQUE KEY  jsid_ (jsid),
    UNIQUE KEY  code_date_type_name (fund, end_date, style_type, style),
    KEY code_ (fund),
    KEY code_date (fund, end_date),
    KEY code_date_type (fund, end_date, style_type),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `zhijunfund_analysis`.`t_sys_seq`(`name`, `value`, `begin`, `step`, `version`, `update_time`, `remark`) VALUES ('fund_style_allocation', 1, 1, 1000, 0, now(), NULL);


create table client_style_allocation(
    id                      bigint(20)         NOT NULL         AUTO_INCREMENT  comment 'ID',
    client_id               int(11)            NOT NULL         comment '内部编码',
    end_date                datetime           NOT NULL         comment '交易日/持仓公布日',
    style_type              varchar(20)        NOT NULL         comment '风格类型',
    style                   varchar(20)        NOT NULL         comment '风格名称',
    weight                  decimal(18, 6)     NOT NULL         comment '风格权重',
    va                      decimal(18, 6)     NOT NULL         comment '风格归因',
    update_time             timestamp          NOT NULL         comment '更新时间'  DEFAULT  CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    jsid                    bigint(20)         NOT NULL,
    UNIQUE KEY  jsid_ (jsid),
    UNIQUE KEY  code_date_type_name (client_id, end_date, style_type, style),
    KEY code_ (client_id),
    KEY code_date (client_id, end_date),
    KEY code_date_type (client_id, end_date, style_type),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `zhijunfund_analysis`.`t_sys_seq`(`name`, `value`, `begin`, `step`, `version`, `update_time`, `remark`) VALUES ('client_style_allocation', 1, 1, 1000, 0, now(), NULL);
