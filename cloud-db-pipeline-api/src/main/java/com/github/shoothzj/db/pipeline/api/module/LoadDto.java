package com.github.shoothzj.db.pipeline.api.module;

import com.github.shoothzj.db.pipeline.api.module.db.DbInfoDto;
import lombok.Data;

import java.util.List;

/**
 * @author hezhangjian
 */
@Data
public class LoadDto {

    private List<LoadAction> loadActions;

    private DbInfoDto dbInfo;

}
