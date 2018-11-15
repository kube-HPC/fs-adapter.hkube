const component = 'FSAdapter';
const path = require('path');
const fs = require('fs-extra');
const recursive = require('recursive-readdir');

class FSAdapter {
    constructor() {
        this._wasInit = false;
    }

    async init(config, log, directories, bootstrap = false) {
        if (!this._wasInit) {
            this._wasInit = true;
            this._log = log;

            if (bootstrap) {
                Object.values(directories).forEach(async (dirName) => {
                    const bn = path.join(dirName);
                    await fs.ensureDir(bn);
                });
            }
        }
    }

    async put(options) {
        return this._put({ ...this._parsePath(options.Path), Data: options.Data });
    }

    async _put(options) {
        const start = Date.now();
        await fs.ensureDir(options.DirectoryName);
        await fs.writeJson(path.join(options.DirectoryName, options.FileName), options.Data);
        const end = Date.now();
        if (this._log) {
            const diff = end - start;
            this._log.debug(`Execution of put takes ${diff} ms`, { component, operation: 'put', time: diff });
        }
        return { Path: path.join(options.DirectoryName, options.FileName) };
    }

    async get(options) {
        const start = Date.now();
        const res = await fs.readJson(options.Path);
        const end = Date.now();

        if (this._log) {
            const diff = end - start;
            this._log.debug(`Execution of get takes ${diff} ms`, { component, operation: 'get', time: diff });
        }

        return res;
    }

    _parsePath(fullPath) {
        const seperatedPath = fullPath.split(path.sep);
        const directoryName = seperatedPath.slice(0, seperatedPath.length - 1);
        const FileName = seperatedPath[seperatedPath.length - 1];
        return { DirectoryName: directoryName.join(path.sep), FileName };
    }

    async list(options) {
        return this._listFiles(options);
    }

    async _listFiles(options) {
        const res = await recursive(options.Path);
        return res.map((p) => {
            return { Path: p };
        });
    }

    async delete(options) {
        return fs.remove(options.Path);
    }
}

module.exports = new FSAdapter();
