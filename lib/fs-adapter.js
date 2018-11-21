const path = require('path');
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
                Object.values(directories).forEach(async (dirName) => {
                    const bn = path.join(this._baseDirectory, dirName);
                    await fs.ensureDir(bn);
                });
            }
        }
    }

    async put(options) {
        return this._put({ ...this._parsePath(options.Path), Data: options.Data });
    }

    async _put(options) {
        await fs.ensureDir(path.join(this._baseDirectory, options.DirectoryName));
        await fs.writeJson(path.join(this._baseDirectory, options.DirectoryName, options.FileName), options.Data);
        return { Path: path.join(options.DirectoryName, options.FileName) };
    }

    async get(options) {
        const res = await fs.readJson(path.join(this._baseDirectory, options.Path));
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
        const root = path.join(this._baseDirectory, options.Path);
        const exists = await fs.pathExists(root);
        if (exists) {
            const res = await recursive(root);
            return res.map((p) => {
                return { Path: p };
            });
        }

        const seperatedPath = root.split(path.sep);
        const directoryName = (seperatedPath.slice(0, seperatedPath.length - 1)).join(path.sep);

        const prefix = seperatedPath[seperatedPath.length - 1];
        const files = await fs.readdir(directoryName);
        const result = [];
        files.forEach((file) => {
            if (!fs.statSync(path.join(directoryName, file)).isDirectory() && file.startsWith(prefix)) {
                result.push({ Path: path.join(directoryName, file) });
            }
        });
        return result;
    }

    async delete(options) {
        return fs.remove(path.join(this._baseDirectory, options.Path));
    }
}

module.exports = new FSAdapter();
