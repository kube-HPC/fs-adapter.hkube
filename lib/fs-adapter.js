const moment = require('moment');
const { DIR_NAMES } = require('../consts/dir-names');
const component = 'FSAdapter';
const path = require('path');
const fs = require('fs-extra');

class FSAdapter {
    constructor() {
        this._wasInit = false;
    }

    get DateFormat() {
        return 'YYYY-MM-DD';
    }

    async init(options, log, bootstrap = false) {
        if (!this._wasInit) {
            this._wasInit = true;
            this._log = log;
            this._dir = options.baseDirectory;

            if (bootstrap) {
                Object.values(DIR_NAMES).forEach(async (dirName) => {
                    const bn = path.join(this._dir, dirName);
                    await fs.ensureDir(bn);
                });
            }
        }
    }

    async putResults(options) {
        return this._put({ DirectoryName: path.join(this._dir, DIR_NAMES.HKUBE_RESULTS, `${moment().format(this.DateFormat)}`, options.jobId), FileName: 'result.json', Data: options.data });
    }

    async put(options) {
        return this._put({ DirectoryName: path.join(this._dir, DIR_NAMES.HKUBE, `${moment().format(this.DateFormat)}`, options.jobId), FileName: options.taskId, Data: options.data });
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
        return { FullPath: path.join(options.DirectoryName, options.FileName) };
    }

    async get(options) {
        const start = Date.now();
        const res = await fs.readJson(options.FullPath);
        const end = Date.now();

        if (this._log) {
            const diff = end - start;
            this._log.debug(`Execution of get takes ${diff} ms`, { component, operation: 'get', time: diff });
        }

        return res;
    }

    async listObjects(options = null) { // eslint-disable-line no-unused-vars
        const directories = await fs.readdir(path.join(this._dir, DIR_NAMES.HKUBE));
        const res = {};
        directories.forEach(d => res[d] = [{ Key: 'not relevant' }]); // eslint-disable-line no-return-assign
        return res;
    }

    async listObjectsResults(options = null) { // eslint-disable-line no-unused-vars
        const directories = await fs.readdir(path.join(this._dir, DIR_NAMES.HKUBE_RESULTS));
        const res = {};
        directories.forEach(d => res[d] = [{ Key: 'not relevant' }]); // eslint-disable-line no-return-assign
        return res;
    }

    async deleteByDate(options) {
        return this._deleteByDate({ ...options, DirectoryName: DIR_NAMES.HKUBE });
    }

    async deleteResultsByDate(options) {
        return this._deleteByDate({ ...options, DirectoryName: DIR_NAMES.HKUBE_RESULTS });
    }


    async _deleteByDate(options) {
        const dateToDelete = moment(options.Date).format(this.DateFormat);
        const dirToDelete = path.join(this._dir, options.DirectoryName, dateToDelete);
        await fs.remove(dirToDelete);
    }

    async jobPath(options) {
        return options;
    }

    async getStream(options) { // eslint-disable-line no-unused-vars
        // NOT IN USE
    }
}

module.exports = new FSAdapter();
