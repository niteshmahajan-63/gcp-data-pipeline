CREATE table `sync_constant.channel` (
    id INT64,
    value STRING,
    channelid STRING,
    insertedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
