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
            this._cleanPathRegex = new RegExp(this._baseDirectory, 'g');
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
        let exists = await fs.pathExists(root);
        if (exists) {
            const res = await recursive(root);
            return res.map((path) => {
                const filteredPath = path.replace(this._cleanPathRegex, '');
                return { path: filteredPath };
            });
        }

        const seperatedPath = root.split(pathLib.sep);
        const directoryName = (seperatedPath.slice(0, seperatedPath.length - 1)).join(pathLib.sep);

        const prefix = seperatedPath[seperatedPath.length - 1];
        exists = await fs.pathExists(directoryName);
        const result = [];
        if (exists) {
            const files = await fs.readdir(directoryName);
            files.forEach((file) => {
                if (!fs.statSync(pathLib.join(directoryName, file)).isDirectory() && file.startsWith(prefix)) {
                    const fullPath = pathLib.join(directoryName, file);
                    const filteredPath = fullPath.replace(this._cleanPathRegex, '');
                    result.push({ path: filteredPath });
                }
            });
        }
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
        const parsePath = this._parsePath(options.path);
        const dir = pathLib.join(this._baseDirectory, parsePath.directoryName);
        await fs.ensureDir(dir);
        const src = pathLib.join(dir, parsePath.fileName);
        await this._writeStream({ readStream: options.data, src });
        return { path: pathLib.join(parsePath.directoryName, parsePath.fileName) };
    }

    _writeStream({ readStream, src }) {
        return new Promise((resolve, reject) => {
            if (!(readStream instanceof Stream)) {
                reject(new TypeError('data must readable stream'));
            }
            else {
                const writeStream = fs.createWriteStream(src);
                readStream.on('error', (err) => {
                    reject(err);
                });
                writeStream.on('error', (err) => {
                    reject(err);
                });
                writeStream.on('close', () => {
                    resolve();
                });
                readStream.pipe(writeStream);
            }
        });
    }
}

module.exports = new FSAdapter();
