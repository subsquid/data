
clippy:
	@cargo clippy -- \
		-A clippy::len_zero \
		-A clippy::len_without_is_empty \
		-A clippy::missing_safety_doc \
		-A clippy::module_inception


clippy-fix:
	@cargo clippy --fix -- \
		-A clippy::len_zero \
		-A clippy::len_without_is_empty \
		-A clippy::missing_safety_doc \
		-A clippy::module_inception


.PHONY: clippy clippy-fix