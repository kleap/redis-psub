const redis = require('redis');
const Promise = require('bluebird');
const { Errors } = require('./constants');

Promise.promisifyAll(redis.RedisClient.prototype);

class RedisStore {
  constructor({ logger, config }) {
    this.logger = logger;
    this.config = config;
    this.client = redis.createClient(config.port, config.host);
    this.client.on('error', (error) => {
      this.logger.error(error);
      throw new Error(`[RedisStore]  ${Errors.DATABASE_ERROR}`);
    });
    this.client.on('connect', () => {
      this.logger.info('Connected to Redis database');
    });
  }

  publish(channel, eventId) {
    this.client.publish(`${channel}`, eventId, () => {
      this.logger.debug(`Published to redis channel [${channel}], eventId [${eventId}]`);
    });
  }

  async addItem(set, item) {
    const client = redis.createClient(this.config);
    const added = await client.saddAsync(set, item);
    return added;
  }

  async checkItem(set, item) {
    const client = redis.createClient(this.config);
    const subscribed = await client.smembersAsync(set, item);
    return subscribed;
  }

  async removeItem(set, item) {
    const client = redis.createClient(this.config);
    const removed = await client.sremAsync(set, item);
    return removed;
  }


  saveLog(channelId, event) {
    return new Promise((resolve, reject) => {
      this.client.multi()
        .lpush(channelId, JSON.stringify(event))
        .expire(channelId, this.config.LOG_MAX_LIFETIME)
        .exec((err, data) => {
          if (err) {
            return reject(err);
          }
          const [index] = data;
          return resolve(index);
        });
    });
  }

  async subscribe(room, listener) {
    const client = redis.createClient(this.config);
    await client.subscribeAsync(room);
    this.logger.debug(`Create subscription to ${room} redis channel`);
    client.on('message', listener);
  }

  async getLog(channelId, eventId) {
    const client = redis.createClient(this.config);
    const data = await client.lindexAsync(channelId, -eventId);
    return JSON.parse(data);
  }

  async getAllLogs(eventId) {
    const items = await this.client.lrangeAsync(eventId, 0, -1);
    return items.map(item => JSON.parse(item));
  }
}

function redisFactory(logger, config) {
  return new RedisStore({ logger: logger || console, config });
}
module.exports = redisFactory;
