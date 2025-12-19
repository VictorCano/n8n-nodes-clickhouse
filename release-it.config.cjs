module.exports = {
	increment: process.env.RELEASE_TYPE || 'patch',
	git: {
		commitMessage: 'chore(release): v${version} [skip ci]',
	},
};
