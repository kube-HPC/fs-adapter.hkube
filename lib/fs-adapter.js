const pathLib = require('path');
const Stream = require('stream');
const fs = require('fs-extra');
const recursive = require('recursive-readdir');

class FSAdapter {
    constructor() {
        this._wasInit = false;
    }

    async init(config, directories, bootstrap = false) {
        if (!this._wasInit) {
            this._wasInit = true;
            this._baseDirectory = config.baseDirectory;

            if (bootstrap) {
                await Promise.all(Object.values(directories).map(dirName => fs.ensureDir(pathLib.join(this._baseDirectory, dirName))));
            }
        }
    }

    async put(options) {
        return this._put({ ...this._parsePath(options.path), data: options.data });
    }

    async _put(options) {
        await fs.ensureDir(pathLib.join(this._baseDirectory, options.directoryName));
        await fs.writeJson(pathLib.join(this._baseDirectory, options.directoryName, options.fileName), options.data);
        return { path: pathLib.join(options.directoryName, options.fileName) };
    }

    async get(options) {
        const res = await fs.readJson(pathLib.join(this._baseDirectory, options.path));
        return res;
    }

    _parsePath(fullPath) {
        const seperatedPath = fullPath.split(pathLib.sep);
        const dirName = seperatedPath.slice(0, seperatedPath.length - 1);
        const fileName = seperatedPath[seperatedPath.length - 1];
        return { directoryName: dirName.join(pathLib.sep), fileName };
    }

    async list(options) {
        return this._listFiles(options);
    }

    async listPrefixes(options) {
        const root = pathLib.join(this._baseDirectory, options.path);
        return fs.readdir(root);
    }

    async _listFiles(options) {
        const root = pathLib.join(this._baseDirectory, options.path);
        const exists = await fs.pathExists(root);
        if (exists) {
            const res = await recursive(root);
            return res.map((path) => {
                return { path };
            });
        }

        const seperatedPath = root.split(pathLib.sep);
        const directoryName = (seperatedPath.slice(0, seperatedPath.length - 1)).join(pathLib.sep);

        const prefix = seperatedPath[seperatedPath.length - 1];
        const files = await fs.readdir(directoryName);
        const result = [];
        files.forEach((file) => {
            if (!fs.statSync(pathLib.join(directoryName, file)).isDirectory() && file.startsWith(prefix)) {
                result.push({ path: pathLib.join(directoryName, file) });
            }
        });
        return result;
    }

    async delete(options) {
        return fs.remove(pathLib.join(this._baseDirectory, options.path));
    }

    async getStream(options) {
        const parsePath = this._parsePath(options.path);
        const file = pathLib.join(this._baseDirectory, parsePath.directoryName, parsePath.fileName);
        return fs.createReadStream(file);
    }

    async putStream(options) {
        return new Promise(async (resolve, reject) => {
            const { path } = options;
            const readable = options.data;
            const parsePath = this._parsePath(path);
            const dir = pathLib.join(this._baseDirectory, parsePath.directoryName);
            await fs.ensureDir(dir);
            const file = pathLib.join(dir, parsePath.fileName);
            const writable = fs.createWriteStream(file);

            if (!(readable instanceof Stream)) {
                throw new TypeError('data must readable stream');
            }

            const stream = readable.pipe(writable);
            stream.on('finish', () => {
                resolve({ path: pathLib.join(parsePath.directoryName, parsePath.fileName) });
            });
        });
    }
}

module.exports = new FSAdapter();
