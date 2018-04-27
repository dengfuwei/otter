package com.alibaba.otter.shared.common.model.config.data.es;

import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;

public class ElasticsearchMediaSource extends DataMediaSource {

    private static final long serialVersionUID = 2878550932723731876L;

    private String clusterNodes;

    public String getClusterNodes() {
        return clusterNodes;
    }

    public void setClusterNodes(String clusterNodes) {
        this.clusterNodes = clusterNodes;
    }
}
