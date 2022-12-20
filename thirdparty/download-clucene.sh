#!/usr/bin/env bash
#
# gh-dl-release! It works!
# 
# This script downloads an asset from latest or specific Github release of a
# private repo. Feel free to extract more of the variables into command line
# parameters.
#
# PREREQUISITES
#
# curl, wget, jq
#
# USAGE
#
# Set all the variables inside the script, make sure you chmod +x it, then
# to download specific version to my_app.tar.gz:
#
#     gh-dl-release 2.1.1 my_app.tar.gz
#
# to download latest version:
#
#     gh-dl-release latest latest.tar.gz
#
# If your version/tag doesn't match, the script will exit with error.

set -e
################################################################
# This script will download all thirdparties and java libraries
# which are defined in *vars.sh*, unpack patch them if necessary.
# You can run this script multi-times.
# Things will only be downloaded, unpacked and patched once.
################################################################
curdir=`dirname "$0"`
curdir=`cd "$curdir"; pwd`

if [[ -z "${DORIS_HOME}" ]]; then
    DORIS_HOME=$curdir/..
fi

# include custom environment variables
if [[ -f ${DORIS_HOME}/custom_env.sh ]]; then
    . ${DORIS_HOME}/custom_env.sh
fi

if [[ -z "${TP_DIR}" ]]; then
    TP_DIR=$curdir
fi

if [ ! -f ${TP_DIR}/vars.sh ]; then
    echo "vars.sh is missing".
    exit 1
fi
. ${TP_DIR}/vars.sh

mkdir -p ${TP_DIR}/src

md5sum_bin=md5sum
if ! command -v ${md5sum_bin} >/dev/null 2>&1; then
    echo "Warn: md5sum is not installed"
    md5sum_bin=""
fi

md5sum_func() {
    local FILENAME=$1
    local DESC_DIR=$2
    local MD5SUM=$3

    if [ "$md5sum_bin" == "" ]; then
       return 0
    else
       md5=`md5sum "$DESC_DIR/$FILENAME"`
       if [ "$md5" != "$MD5SUM  $DESC_DIR/$FILENAME" ]; then
           echo "$DESC_DIR/$FILENAME md5sum check failed!"
           echo -e "except-md5 $MD5SUM \nactual-md5 $md5"
           return 1
       fi
    fi

    return 0
}

download_clucene() {
    #TOKEN="$3"
    while [[ x"$TOKEN" == x"" ]]; do
            read -p "please enter your github token :" TOKEN
	    sleep 1
    done
    echo "your github token is $TOKEN"
    REPO="selectdb/clucene"
    FILE="clucene.zip"      # the name of your release asset file, e.g. build.tar.gz
    VERSION=$1                       # tag name or the word "latest"
    GITHUB="https://api.github.com"
    
    function gh_curl() {
      /usr/bin/curl -H "Authorization: token $TOKEN" \
           -H "Accept: application/vnd.github.v3.raw" \
           $@
    }
    
    parser=".assets | map(select(.name == \"$FILE\"))[0].id"
    if [ "$VERSION" = "latest" ]; then
      # Github should return the latest release first.
      message=`gh_curl -s $GITHUB/repos/$REPO/releases/latest | jq ".message"`
      if [ "$message" = '"Bad credentials"' ]; then
          echo "ERROR: please check your token or you may not have the right to access"
          exit 1
      fi;
      asset_id=`gh_curl -s $GITHUB/repos/$REPO/releases/latest | jq "$parser"`
    else
      message=`gh_curl -s $GITHUB/repos/$REPO/releases/tags/$VERSION | jq ".message"`
      if [ "$message" = '"Bad credentials"' ]; then
          echo "ERROR: please check your token or you may not have the right to access"
          exit 1
      fi;
      asset_id=`gh_curl -s $GITHUB/repos/$REPO/releases/tags/$VERSION | jq "$parser"`
    fi;
    
    if [ -z "$asset_id" ]; then
      echo "ERROR: version not found $VERSION"
      exit 1
    fi;
    if [ "$asset_id" = "null" ]; then
      echo "ERROR: file $FILE not found in version $VERSION"
      exit 2
    fi;
    wget --auth-no-challenge --header='Accept:application/octet-stream' \
      https://$TOKEN:@api.github.com/repos/$REPO/releases/assets/$asset_id \
      -O $TP_SOURCE_DIR/$CLUCENE_NAME
}
# return 0 if download succeed.
# return 1 if not.
download_func() {
    local FILENAME=$1
    local DESC_DIR=$2
    local MD5SUM=$3

    echo "$FILENAME $DESC_DIR  $MD5SUM"
    if [ -z "$FILENAME" ]; then
        echo "Error: No file name specified to download"
        exit 1
    fi
    if [ -z "$DESC_DIR" ]; then
        echo "Error: No dest dir specified for ${FILENAME} in clucene"
        exit 1
    fi


    local STATUS=1
    for attemp in 1 2; do
        if [ -r "$DESC_DIR/$FILENAME" ]; then
            if md5sum_func $FILENAME $DESC_DIR $MD5SUM; then
                echo "Archive $FILENAME already exist."
                STATUS=0
                break;
            fi
            echo "Archive $FILENAME will be removed and download again."
            rm -f "$DESC_DIR/$FILENAME"
        else
            echo "Downloading $FILENAME from github to $DESC_DIR"
            download_clucene $CLUCENE_VERSION
            if [ "$?"x == "0"x ]; then
                if md5sum_func $FILENAME $DESC_DIR $MD5SUM; then
                    STATUS=0
                    echo "Success to download $FILENAME"
                    break;
                fi
                echo "Archive $FILENAME will be removed and download again."
                rm -f "$DESC_DIR/$FILENAME"
            else
                echo "Failed to download $FILENAME. attemp: $attemp"
            fi
        fi
    done

    if [ $STATUS -ne 0 ]; then
        echo "Failed to download $FILENAME"
    fi
    return $STATUS
}

if ! command -v jq &> /dev/null
then
    echo "jq could not be found, which is needed by clucene downloader, please install it first."
    exit 1
fi

download_func ${CLUCENE_NAME} ${TP_SOURCE_DIR} ${CLUCENE_MD5SUM}
if [ "$?"x != "0"x ]; then
    echo "Failed to download ${CLUCENE_NAME}"
    exit 1
fi
UNZIP_CMD="unzip"
if ! $UNZIP_CMD -o -qq "$TP_SOURCE_DIR/${CLUCENE_NAME}" -d "$TP_SOURCE_DIR/${CLUCENE_SOURCE}"; then
    echo "Failed to unzip ${CLUCENE_NAME}"
    exit 1
fi

