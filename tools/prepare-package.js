const DIST_FOLDER = "dist/"
const PACKAGE_FILE_LOCATION = "package.json";

const fs = require("fs");

const packageJson = JSON.parse(fs.readFileSync(PACKAGE_FILE_LOCATION, "utf-8"));

delete packageJson["files"];
packageJson["main"] = packageJson["main"].replace("dist/", "");

fs.writeFileSync(
  DIST_FOLDER + PACKAGE_FILE_LOCATION,
  JSON.stringify(packageJson, null, 2)
);


