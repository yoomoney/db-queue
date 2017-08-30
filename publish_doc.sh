#!/bin/bash

if [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_BRANCH" == "master" ]; then

    echo -e "Publishing javadoc...\n"

    mkdir -p $HOME/javadoc-latest
    cp -R build/docs/javadoc/* $HOME/javadoc-latest

    cd $HOME
    git clone --branch=gh-pages https://${GITHUB_TOKEN}@github.com/${TRAVIS_REPO_SLUG} gh-pages

    cd gh-pages
    git rm -rf ./*
    cp -Rf $HOME/javadoc-latest/* ./
    git add -f .
    git commit -m "Javadoc from $TRAVIS_BRANCH on build $TRAVIS_BUILD_NUMBER auto-pushed to gh-pages"
    git push -fq origin gh-pages

    echo -e "Published Javadoc to gh-pages.\n"

fi