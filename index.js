'use strict';

const contimer = require('contimer');
const crypto = require('crypto');
const de = require('descript');
const Memcached = require('memcached');

class DescriptMemcached {

    constructor(options, logger) {
        this._options = Object.assign({
            defaultKeyTTL: 60 * 60 * 24, // key ttl in seconds
            generation: 1, // increment generation to invalidate all key across breaking changes releases
            readTimeout: 100, // read timeout in milliseconds,
            memcachedOptions: {}, // @see https://github.com/3rd-Eden/memcached#options
        }, options);

        this._logger = logger;
        this._client = new Memcached(options.servers, options.memcachedOptions);
        this._log({
            type: DescriptMemcached.EVENT.MEMCACHED_INITIALIZED,
            options: options,
        });
    }

    get({ key, context }) {
        const normalizedKey = this.normalizeKey(key);

        return new Promise((resolve, reject) => {
            this._log({
                type: DescriptMemcached.EVENT.MEMCACHED_READ_START,
                key,
                normalizedKey,
            }, context);

            const networkTimerStop = contimer.start({}, 'descript-memcached.get.network');
            const totalTimerStop = contimer.start({}, 'descript-memcached.get.total');

            let isTimeout = false;

            const timer = setTimeout(() => {
                isTimeout = true;

                const networkTimer = networkTimerStop();
                const totalTimer = totalTimerStop();

                this._log({
                    type: DescriptMemcached.EVENT.MEMCACHED_READ_TIMEOUT,
                    key,
                    normalizedKey,
                    timers: {
                        network: networkTimer,
                        total: totalTimer,
                    },
                }, context);

                reject(de.error({
                    id: DescriptMemcached.EVENT.MEMCACHED_READ_TIMEOUT,
                }));
            }, this._options.readTimeout);

            this._client.get(normalizedKey, (error, data) => {
                if (isTimeout) {
                    return;
                }

                const networkTimer = networkTimerStop();
                clearTimeout(timer);

                if (error) {
                    const totalTimer = totalTimerStop();
                    this._log({
                        type: DescriptMemcached.EVENT.MEMCACHED_READ_ERROR,
                        error,
                        key,
                        normalizedKey,
                        timers: {
                            network: networkTimer,
                            total: totalTimer,
                        },
                    }, context);

                    reject(de.error({
                        id: DescriptMemcached.EVENT.MEMCACHED_READ_ERROR,
                    }));
                } else if (!data) {
                    const totalTimer = totalTimerStop();
                    this._log({
                        type: DescriptMemcached.EVENT.MEMCACHED_READ_KEY_NOT_FOUND,
                        key,
                        normalizedKey,
                        timers: {
                            network: networkTimer,
                            total: totalTimer,
                        },
                    }, context);

                    reject(de.error({
                        id: DescriptMemcached.EVENT.MEMCACHED_READ_KEY_NOT_FOUND,
                    }));
                } else {
                    let parsedValue;
                    try {
                        parsedValue = JSON.parse(data);
                    } catch (error) {
                        const totalTimer = totalTimerStop();
                        this._log({
                            type: DescriptMemcached.EVENT.MEMCACHED_JSON_PARSING_FAILED,
                            data,
                            error,
                            key,
                            normalizedKey,
                            timers: {
                                network: networkTimer,
                                total: totalTimer,
                            },
                        }, context);

                        reject(de.error({
                            id: DescriptMemcached.EVENT.MEMCACHED_JSON_PARSING_FAILED,
                        }));
                        return;
                    }

                    const totalTimer = totalTimerStop();
                    this._log({
                        type: DescriptMemcached.EVENT.MEMCACHED_READ_DONE,
                        data,
                        key,
                        normalizedKey,
                        timers: {
                            network: networkTimer,
                            total: totalTimer,
                        },
                    }, context);

                    resolve(parsedValue);
                }
            });
        });
    }

    set({ key, value, maxage = this._options.defaultKeyTTL, context } ) {
        if (typeof value === 'undefined') {
            return;
        }

        const totalTimerStop = contimer.start({}, 'descript-memcached.set.total');
        const normalizedKey = this.normalizeKey(key);

        return new Promise((resolve, reject) => {
            this._log({
                type: DescriptMemcached.EVENT.MEMCACHED_WRITE_START,
                key,
                normalizedKey,
            }, context);

            // save to memcached only serializable data
            const safeMemcachedValue = {
                status_code: value.status_code,
                headers: value.headers,
                result: value.result,
            };

            let json;
            try {
                json = JSON.stringify(safeMemcachedValue);
            } catch (error) {
                const totalTimer = totalTimerStop();
                this._log({
                    type: DescriptMemcached.EVENT.MEMCACHED_JSON_STRINGIFY_FAILED,
                    data: value,
                    error,
                    key,
                    normalizedKey,
                    timers: {
                        network: {},
                        total: totalTimer,
                    },
                }, context);
                reject(de.error({
                    id: DescriptMemcached.EVENT.MEMCACHED_JSON_STRINGIFY_FAILED,
                }));
                return;
            }

            const networkTimerStop = contimer.start({}, 'descript-memcached.set.network');
            // maxage - seconds
            this._client.set(normalizedKey, json, maxage, (error, done) => {
                const networkTimer = networkTimerStop();
                const totalTimer = totalTimerStop();
                if (error) {
                    this._log({
                        type: DescriptMemcached.EVENT.MEMCACHED_WRITE_ERROR,
                        error,
                        key,
                        normalizedKey,
                        timers: {
                            network: networkTimer,
                            total: totalTimer,
                        },
                    }, context);
                    reject(de.error({
                        id: DescriptMemcached.EVENT.MEMCACHED_WRITE_ERROR,
                    }));
                } else if (!done) {
                    this._log({
                        type: DescriptMemcached.EVENT.MEMCACHED_WRITE_FAILED,
                        key,
                        normalizedKey,
                        timers: {
                            network: networkTimer,
                            total: totalTimer,
                        },
                    }, context);
                    reject(de.error({
                        id: DescriptMemcached.EVENT.MEMCACHED_WRITE_FAILED,
                    }));
                } else {
                    this._log({
                        type: DescriptMemcached.EVENT.MEMCACHED_WRITE_DONE,
                        data: json,
                        key,
                        normalizedKey,
                        timers: {
                            network: networkTimer,
                            total: totalTimer,
                        },
                    }, context);
                    resolve();
                }
            });
        });
    }

    /**
     * Generates normalized SHA-512 key with generation
     * @param {string} key
     * @returns {string}
     */
    normalizeKey(key) {
        const value = `g${ this._options.generation }:${ key }`;
        return crypto
            .createHash('sha512')
            .update(value, 'utf8')
            .digest('hex');
    }

    _log(event, context) {
        if (this._logger) {
            this._logger.log(event, context);
        }
    }
}

DescriptMemcached.EVENT = {
    MEMCACHED_INITIALIZED: 'MEMCACHED_INITIALIZED',

    MEMCACHED_JSON_PARSING_FAILED: 'MEMCACHED_JSON_PARSING_FAILED',
    MEMCACHED_JSON_STRINGIFY_FAILED: 'MEMCACHED_JSON_STRINGIFY_FAILED',

    MEMCACHED_READ_DONE: 'MEMCACHED_READ_DONE',
    MEMCACHED_READ_ERROR: 'MEMCACHED_READ_ERROR',
    MEMCACHED_READ_KEY_NOT_FOUND: 'MEMCACHED_READ_KEY_NOT_FOUND',
    MEMCACHED_READ_START: 'MEMCACHED_READ_START',
    MEMCACHED_READ_TIMEOUT: 'MEMCACHED_READ_TIMEOUT',

    MEMCACHED_WRITE_DONE: 'MEMCACHED_WRITE_DONE',
    MEMCACHED_WRITE_ERROR: 'MEMCACHED_WRITE_ERROR',
    MEMCACHED_WRITE_FAILED: 'MEMCACHED_WRITE_FAILED',
    MEMCACHED_WRITE_START: 'MEMCACHED_READ_START',
};

module.exports = DescriptMemcached;
