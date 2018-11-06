const moment = require('moment');
const fs = require('fs');
const { DIR_NAMES } = require('../consts/dir-names');
const component = 'FSAdapter';
const path = require('path');
const mkdirp = require('mkdirp');

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
            this._dir = options.Directory;

            if (bootstrap) {
                await Promise.all(Object.values(DIR_NAMES).map((dirName) => { // eslint-disable-line array-callback-return
                    const bn = path.join(this._dir, dirName);
                    if (!fs.existsSync(bn)) {
                        mkdirp(bn);
                    }
                }));
            }
        }
    }

    async putResults(options) {
        return this._put({ DirectoryName: path.join(this._dir, DIR_NAMES.HKUBE_RESULTS, options.jobId), FileName: 'result.json', Data: JSON.stringify(options.data) });
    }

    async put(options) {
        return this._put({ DirectoryName: path.join(this._dir, DIR_NAMES.HKUBE, options.jobId.toString()), FileName: options.taskId, Data: JSON.stringify(options.data) });
    }

    async _put(options) {
        const start = Date.now();
        if (!fs.existsSync(options.DirectoryName)) {
            fs.mkdirSync(options.DirectoryName);
        }
        fs.writeFileSync(path.join(options.DirectoryName, options.FileName), options.Data);
        const end = Date.now();
        if (this._log) {
            const diff = end - start;
            this._log.debug(`Execution of put takes ${diff} ms`, { component, operation: 'put', time: diff });
        }
        return { FullPath: path.join(options.DirectoryName, options.FileName) };
    }

    async get(options) {
        const start = Date.now();
        const res = JSON.parse(fs.readFileSync(options.FullPath, 'utf8'));
        const end = Date.now();

        if (this._log) {
            const diff = end - start;
            this._log.debug(`Execution of get takes ${diff} ms`, { component, operation: 'get', time: diff });
        }

        return res;
    }

    async listObjects(options = null) { // eslint-disable-line no-unused-vars
        throw new Error('not implemented');
    }

    async listObjectsResults(options = null) { // eslint-disable-line no-unused-vars
        throw new Error('not implemented');
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
        if (!fs.existsSync(dirToDelete)) {
            fs.removeSync(dirToDelete);
        }
    }

    async jobPath(options) {
        return options;
    }

    async getStream(options) { // eslint-disable-line no-unused-vars
        // NOT IN USE
    }
}

module.exports = new FSAdapter();
